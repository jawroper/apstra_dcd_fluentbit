// main.go — Fluent Bit Go input plugin for Apstra DCD telemetry streaming.
//
// Build as a shared library:
//
//	go build -buildmode=c-shared -o apstra_dcd_fluentbit.so .
//
// Then reference it in fluent-bit.conf:
//
//	[INPUT]
//	    Name   apstra_dcd
//	    ...
package main

import "C"

import (
	"context"
	"log"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/input"

	"github.com/jawroper/apstra-dcd-fluentbit/pkg/cbuf"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/config"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder"
	v6_0_0 "github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder/v6_0_0"
	v6_1_2 "github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder/v6_1_2"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/listener"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/promexport"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/restapi"
)

// Version is set at build time via -ldflags="-X main.Version=<value>".
// Use: make build VERSION=1.2.3
var Version = "dev"

// releaseHandlers maps the `dcd_release` config value to the handler
// constructor from the matching pkg/decoder/vX_Y_Z package. To support a new
// DCD release: add its proto package + decoder package (copy an existing one
// as a template — see proto/README.md), then add one line here.
var releaseHandlers = map[string]func(*decoder.Decoder) func([]byte) ([]decoder.Record, error){
	v6_0_0.Release: v6_0_0.NewHandler,
	v6_1_2.Release: v6_1_2.NewHandler,
}

func supportedReleases() []string {
	out := make([]string, 0, len(releaseHandlers))
	for r := range releaseHandlers {
		out = append(out, r)
	}
	return out
}

// pluginInstance holds all runtime state for one loaded instance of the plugin.
type pluginInstance struct {
	cfg          *config.Config
	api          *restapi.Client
	listener     *listener.Listener
	promExporter *promexport.Exporter // nil unless cfg.PrometheusEnabled
	quit         chan struct{}
}

var instances = map[unsafe.Pointer]*pluginInstance{}
var ctxCallCount int64
var ctxMissCount int64

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return input.FLBPluginRegister(def, "apstra_dcd", "Apstra DCD Telemetry Streaming Input")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	log.Printf("I! [dcd] apstra_dcd plugin version %s", Version)
	cfg, err := config.FromPlugin(plugin)
	if err != nil {
		log.Printf("E! [dcd] Configuration error: %v", err)
		return input.FLB_ERROR
	}

	log.Printf("I! [dcd] Initializing — DCD server: %s://%s:%d  listen port: %d",
		cfg.DcdProtocol, cfg.DcdServer, cfg.DcdPort, cfg.Port)

	api := restapi.NewClient(cfg.DcdServer, cfg.DcdPort, cfg.DcdLogin, cfg.DcdPassword, cfg.DcdProtocol)

	if err := api.Login(); err != nil {
		log.Printf("W! [dcd] Could not login to DCD: %v — continuing without metadata enrichment", err)
	} else {
		if err := api.GetBlueprints(); err != nil {
			log.Printf("W! [dcd] GetBlueprints: %v", err)
		}
		if err := api.GetSystems(); err != nil {
			log.Printf("W! [dcd] GetSystems: %v", err)
		}
	}

	dec := decoder.New(api, cfg)
	log.Printf("I! [dcd] Output format: %s", cfg.OutputFormat)

	var promExporter *promexport.Exporter
	if cfg.PrometheusEnabled {
		promExporter = promexport.New(cfg.PrometheusStreamingTypes)
		if err := promExporter.Start(cfg.PrometheusPort); err != nil {
			log.Printf("E! [dcd] Could not start Prometheus exporter: %v", err)
			return input.FLB_ERROR
		}
		log.Printf("I! [dcd] Prometheus exporter exporting types: %v", cfg.PrometheusStreamingTypes)

		// If prometheus_streaming_types requests a type that streaming_types
		// doesn't subscribe to, automatically add it — otherwise DCD would
		// never send that data and Prometheus would silently export nothing for
		// it. The extra type flows in on the same TCP listener and is available
		// to Fluent Bit's normal pipeline too; whether it reaches a [OUTPUT]
		// depends on your Match/tag routing as usual.
		subscribed := make(map[string]bool)
		for _, t := range cfg.StreamingTypes {
			subscribed[strings.TrimSpace(strings.ToLower(t))] = true
		}
		for _, t := range cfg.PrometheusStreamingTypes {
			t = strings.TrimSpace(strings.ToLower(t))
			if !subscribed[t] {
				log.Printf("I! [dcd] prometheus_streaming_types includes %q which is not in streaming_types — "+
					"automatically adding it so DCD will send this data to the Prometheus exporter", t)
				cfg.StreamingTypes = append(cfg.StreamingTypes, t)
				subscribed[t] = true
			}
		}

		dec.SetMetricsObserver(promExporter)
	}

	newHandler, ok := releaseHandlers[cfg.DcdRelease]
	if !ok {
		log.Printf("E! [dcd] Unsupported dcd_release %q — supported releases: %v", cfg.DcdRelease, supportedReleases())
		return input.FLB_ERROR
	}
	log.Printf("I! [dcd] Decoding as DCD release %s", cfg.DcdRelease)

	lst := listener.New(cfg.Port, newHandler(dec), cfg.Debug)
	if err := lst.Start(); err != nil {
		log.Printf("E! [dcd] Could not start TCP listener: %v", err)
		return input.FLB_ERROR
	}

	for _, st := range cfg.StreamingTypes {
		if err := api.StartStreaming(st, cfg.LocalAddress, cfg.Port); err != nil {
			log.Printf("W! [dcd] StartStreaming(%s): %v", st, err)
		}
	}

	quit := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.RefreshInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := api.GetBlueprints(); err != nil {
					log.Printf("W! [dcd] Refresh GetBlueprints: %v", err)
				}
				if err := api.GetSystems(); err != nil {
					log.Printf("W! [dcd] Refresh GetSystems: %v", err)
				}
				log.Printf("D! [dcd] Metadata refreshed")
			case <-quit:
				return
			}
		}
	}()

	inst := &pluginInstance{
		cfg:          cfg,
		api:          api,
		listener:     lst,
		promExporter: promExporter,
		quit:         quit,
	}
	instances[plugin] = inst
	log.Printf("D! [dcd] Registered plugin instance at ptr=%p", plugin)

	log.Printf("I! [dcd] Plugin initialized — streaming types: %v", cfg.StreamingTypes)
	return input.FLB_OK
}

//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, size *C.size_t) int {
	var inst *pluginInstance
	for _, v := range instances {
		inst = v
		break
	}
	if inst == nil {
		return input.FLB_OK
	}

	const maxBatchSize = 1000
	enc := input.NewEncoder()
	count := 0
	var packed []byte

	for count < maxBatchSize {
		select {
		case rec := <-inst.listener.Records:
			ts := input.FLBTime{Time: time.Now()}
			if raw, ok := rec["_ts"]; ok {
				if ns, ok := raw.(int64); ok {
					ts = input.FLBTime{Time: time.Unix(0, ns)}
				}
				delete(rec, "_ts")
			}
			entry := []interface{}{ts, map[string]interface{}(rec)}
			b, err := enc.Encode(entry)
			if err != nil {
				log.Printf("W! [dcd] Failed to encode record: %v", err)
				continue
			}
			packed = append(packed, b...)
			count++
		default:
			goto done
		}
	}

done:
	if count > 0 {
		cBuf := cbuf.ToCBuffer(packed)
		if cBuf == nil {
			log.Printf("E! [dcd] Could not allocate C buffer for %d-byte record batch (count=%d) — dropping batch", len(packed), count)
			return input.FLB_OK
		}
		*data = cBuf
		*size = C.size_t(len(packed))
		if inst.cfg.Debug {
			log.Printf("D! [dcd] Emitting %d records to Fluent Bit", count)
		}
	}

	return input.FLB_OK
}

//export FLBPluginInputCallbackCtx
func FLBPluginInputCallbackCtx(ctx unsafe.Pointer, data *unsafe.Pointer, size *C.size_t) int {
	inst, ok := instances[ctx]
	if !ok {
		// Observed in the field: the ctx pointer Fluent Bit hands back to this
		// callback does not always match the `plugin` pointer passed to
		// FLBPluginInit on this build/version. Rather than silently drop every
		// batch, fall back to the single registered instance — correct for the
		// overwhelmingly common case of one [INPUT] apstra_dcd block. If more
		// than one instance is registered we can't safely guess, so we still
		// bail out in that case.
		if len(instances) == 1 {
			for _, v := range instances {
				inst = v
			}
		} else {
			return input.FLB_OK
		}
		n := atomic.AddInt64(&ctxMissCount, 1)
		if inst.cfg.Debug && n%100 == 1 {
			log.Printf("W! [dcd] InputCallbackCtx: ctx %p not found in instances map (have %d registered) — miss #%d", ctx, len(instances), n)
		}
	}

	n := atomic.AddInt64(&ctxCallCount, 1)
	if inst.cfg.Debug && n%100 == 1 {
		log.Printf("D! [dcd] (Ctx) heartbeat: call #%d, %d records currently queued", n, len(inst.listener.Records))
	}

	const maxBatchSize = 1000
	enc := input.NewEncoder()
	count := 0
	var packed []byte

	for count < maxBatchSize {
		select {
		case rec := <-inst.listener.Records:
			ts := input.FLBTime{Time: time.Now()}
			if raw, ok := rec["_ts"]; ok {
				if ns, ok := raw.(int64); ok {
					ts = input.FLBTime{Time: time.Unix(0, ns)}
				}
				delete(rec, "_ts")
			}
			entry := []interface{}{ts, map[string]interface{}(rec)}
			b, err := enc.Encode(entry)
			if err != nil {
				log.Printf("W! [dcd] Failed to encode record: %v", err)
				continue
			}
			packed = append(packed, b...)
			count++
		default:
			goto done
		}
	}

done:
	if count > 0 {
		cBuf := cbuf.ToCBuffer(packed)
		if cBuf == nil {
			log.Printf("E! [dcd] Could not allocate C buffer for %d-byte record batch (count=%d) — dropping batch", len(packed), count)
			return input.FLB_OK
		}
		*data = cBuf
		*size = C.size_t(len(packed))
		if inst.cfg.Debug {
			log.Printf("D! [dcd] (Ctx) Emitting %d records to Fluent Bit — %d still queued", count, len(inst.listener.Records))
		}
	}
	return input.FLB_OK
}

// FLBPluginInputCleanupCallback is called by Fluent Bit's C engine once it's
// done reading a buffer previously returned via FLBPluginInputCallback(Ctx)'s
// *data out-parameter, so we can free the C.malloc'd memory from toCBuffer.
// Without this, every batch handed back to Fluent Bit would leak.
//
//export FLBPluginInputCleanupCallback
func FLBPluginInputCleanupCallback(data unsafe.Pointer) int {
	cbuf.Free(data)
	return input.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	for _, inst := range instances {
		close(inst.quit)
		inst.listener.Stop()
		if err := inst.api.StopStreaming(); err != nil {
			log.Printf("W! [dcd] StopStreaming: %v", err)
		}
		if inst.promExporter != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := inst.promExporter.Stop(ctx); err != nil {
				log.Printf("W! [dcd] Prometheus exporter shutdown: %v", err)
			}
			cancel()
		}
	}
	instances = map[unsafe.Pointer]*pluginInstance{}
	log.Printf("I! [dcd] Plugin exited cleanly")
	return input.FLB_OK
}

func main() {}
