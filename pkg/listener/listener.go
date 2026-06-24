// Package listener implements the TCP server that receives DCD telemetry
// streaming data.
//
// Wire format (from dcd.go MsgReader):
//
//	[2 bytes big-endian uint16 = N][N bytes serialized DcdMessage protobuf]
//
// Each accepted TCP connection is handled in its own goroutine.
// Decoded records are sent on the Records channel for the plugin to consume.
//
// The listener itself knows nothing about protobuf or which DCD release's
// schema applies — that's entirely encapsulated in the MessageHandler passed
// to New, which is built by the matching pkg/decoder/vX_Y_Z package for
// whatever the [INPUT] config's dcd_release selects. This keeps the listener
// reusable across every DCD release without modification.
package listener

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"

	"github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder"
)

// MessageHandler unmarshals one raw DCD wire message and decodes it into
// Fluent Bit records. Each pkg/decoder/vX_Y_Z package provides one of these
// (see NewHandler in that package) bound to the correct generated proto types
// for that DCD release.
type MessageHandler func(b []byte) ([]decoder.Record, error)

// Listener accepts DCD telemetry TCP connections and decodes messages.
// msgReadCount is a diagnostic counter, logged periodically to confirm
// real message bytes are being read off the wire and to show their raw
// content for troubleshooting unexpected schema/framing mismatches.
var msgReadCount int64

type Listener struct {
	port    int
	handler MessageHandler
	debug   bool                // gates verbose diagnostic logging (see config.Config.Debug)
	Records chan decoder.Record // decoded records ready for Fluent Bit
	quit    chan struct{}
	ln      net.Listener
}

// New creates a Listener on the given port that decodes messages using
// handler. debug gates verbose diagnostic logging (raw message byte
// previews) — leave false in normal production use.
func New(port int, handler MessageHandler, debug bool) *Listener {
	return &Listener{
		port:    port,
		handler: handler,
		debug:   debug,
		Records: make(chan decoder.Record, 10000),
		quit:    make(chan struct{}),
	}
}

// Start binds the TCP port and begins accepting connections.
func (l *Listener) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", l.port))
	if err != nil {
		return fmt.Errorf("listener: bind port %d: %w", l.port, err)
	}
	l.ln = ln
	log.Printf("I! [dcd] Listening for DCD telemetry on TCP port %d", l.port)
	go l.acceptLoop()
	return nil
}

// Stop closes the listener and signals goroutines to exit.
func (l *Listener) Stop() {
	close(l.quit)
	if l.ln != nil {
		l.ln.Close()
	}
}

// acceptLoop accepts incoming connections — mirrors ssl.listen() in dcd.go.
func (l *Listener) acceptLoop() {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			select {
			case <-l.quit:
				return
			default:
				log.Printf("W! [dcd] Accept error: %v", err)
				continue
			}
		}
		go l.msgReader(conn)
	}
}

// msgReader is a direct port of MsgReader() in dcd.go.
// Reads 2-byte big-endian length prefix → reads body → hands off to the
// version-specific handler for unmarshal + decode.
func (l *Listener) msgReader(conn net.Conn) {
	defer conn.Close()
	log.Printf("D! [dcd] TCP session opened from %s", conn.RemoteAddr())

	for {
		// 1. Read the 2-byte big-endian length header
		var msgSize uint16
		if err := binary.Read(conn, binary.BigEndian, &msgSize); err != nil {
			if err != io.EOF {
				log.Printf("W! [dcd] Reading message size: %v", err)
			}
			return
		}

		if msgSize == 0 {
			log.Printf("W! [dcd] Zero-length message, skipping")
			continue
		}

		// 2. Read exactly msgSize bytes
		msgBuf := make([]byte, msgSize)
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			log.Printf("W! [dcd] Reading %d-byte message body: %v", msgSize, err)
			return
		}

		if l.debug {
			n := atomic.AddInt64(&msgReadCount, 1)
			if n%50 == 1 {
				preview := msgBuf
				if len(preview) > 24 {
					preview = preview[:24]
				}
				log.Printf("D! [dcd] Read message #%d: size=%d first_bytes=%x", n, msgSize, preview)
			}
		}

		// 3. Unmarshal + decode via the version-specific handler
		records, err := l.handler(msgBuf)
		if err != nil {
			log.Printf("W! [dcd] Unmarshal error: %v", err)
			continue
		}

		// 4. Enqueue decoded records
		for _, rec := range records {
			select {
			case l.Records <- rec:
			case <-l.quit:
				return
			default:
				log.Printf("W! [dcd] Record channel full — dropping (series=%v)", rec["series"])
			}
		}
	}
}
