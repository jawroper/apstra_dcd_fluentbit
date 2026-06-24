# Supporting multiple DCD releases

DCD renumbers — and sometimes restructures — its streaming telemetry
protobuf schema between releases. Field *names* have stayed stable across
the two releases supported so far, but field *numbers* (the only thing that
actually matters on the wire) do change, so a single schema can only
correctly decode wire bytes from the one DCD version it was generated from.

Each `[INPUT]` block in `fluent-bit.conf` talks to exactly one DCD server, so
it declares which release that server runs via the **`dcd_release`** config
key:

```
[INPUT]
    Name          apstra_dcd
    dcd_release   6.0.0
    dcd_server    192.168.57.250
    local_address 192.168.57.128
    port          7777
```

If you run multiple `[INPUT] apstra_dcd` blocks against different DCD
servers, each can independently declare its own `dcd_release`.

## Naming convention

Every supported release gets its own directory, Go package, and protobuf
package — all three derived mechanically from the dotted version string:

| DCD version string (`dcd_release`) | Directory       | Go package | Source filename                                  | protobuf `package`     |
|-------------------------------------|-----------------|------------|---------------------------------------------------|-------------------------|
| `6.0.0`                              | `proto/v6_0_0/`  | `v6_0_0`   | `streaming-telemetry-schema-v6_0_0.proto`          | `dcd.streaming.v6_0_0`  |
| `6.1.2`                              | `proto/v6_1_2/`  | `v6_1_2`   | `streaming-telemetry-schema-v6_1_2.proto`          | `dcd.streaming.v6_1_2`  |

i.e. `v<major>_<minor>_<patch>`, replacing every `.` with `_` and prefixing
`v`. The matching decoder package lives at `pkg/decoder/v<major>_<minor>_<patch>/`.

**Why the protobuf `package` and source filename must also be unique** (not
just the Go package/directory): every supported release is compiled into the
*same* `apstra_dcd_fluentbit.so`, and `google.golang.org/protobuf` keeps a single
global registry of every linked-in message type, keyed by its fully-qualified
protobuf name and source file path — not by Go import path. If two versions
both declared `package dcd.streaming;` in a file both calling itself
`streaming-telemetry-schema.proto`, the binary would panic at startup with a
duplicate-registration error the moment both were imported together. Giving
each version a distinct protobuf `package` and source filename avoids this
entirely.

## Adding a new DCD release

Say DCD ships `6.2.0` and you've downloaded its `.proto` schema from a live
server running that version:

1. **Create the directory and place the schema:**
   ```bash
   mkdir -p proto/v6_2_0
   cp downloaded-schema.proto proto/v6_2_0/streaming-telemetry-schema-v6_2_0.proto
   ```

2. **Make the protobuf `package` unique** (required — see above):
   ```bash
   sed -i 's/^package dcd\.streaming;/package dcd.streaming.v6_2_0;/' \
       proto/v6_2_0/streaming-telemetry-schema-v6_2_0.proto
   ```

3. **Defensively relax `required` fields to `optional`**, if the schema uses
   proto2 `required`. DCD's actual sender does not always reliably populate
   every field its own schema marks required (this caused real unmarshal
   failures against the 6.0.0 schema — see the `origin_name` note in
   `proto/v6_0_0/streaming-telemetry-schema-v6_0_0.proto`). Wire encoding is
   identical either way, so this only relaxes *our* parse-time strictness,
   never compatibility with what DCD actually sends:
   ```bash
   sed -i -E 's/^([[:space:]]*)required /\1optional /' \
       proto/v6_2_0/streaming-telemetry-schema-v6_2_0.proto
   ```

4. **Generate the Go bindings:**
   ```bash
   make proto VERSION=v6_2_0
   ```

5. **Create the decoder package** — copy an existing version's `decoder.go`
   wholesale (`pkg/decoder/v6_0_0/decoder.go` is the simpler template if the
   new schema also uses a `uint64` timestamp; `pkg/decoder/v6_1_2/decoder.go`
   if it still uses `google.protobuf.Timestamp`), then:
   - change the `package` declaration and import path to `v6_2_0`
   - update `const Release = "..."`
   - compare the new schema's `Event`/`Alert`/`PerfMon` oneof field *names*
     (not numbers — those don't matter to Go code) against the template's
     case lists, and add/remove any that changed. `go build` will fail
     loudly on any renamed or removed accessor method, so this step is
     mostly self-checking.
   - **confirm `DecodeMessage`'s timestamp handling still calls
     `decoder.SanitizeTimestamp(...)`** around whatever conversion produces
     the candidate `time.Time` (`time.UnixMicro(...)` or `t.AsTime()`,
     depending on the schema). Both reference templates already do this —
     it'll come along automatically if you copied one of them wholesale as
     instructed above — but it's easy to silently lose if you instead
     hand-write the timestamp block from scratch or copy from an older/
     stale local file. Without it, a single DCD clock-source bug (a real
     one was observed in production — see README.md's "Timestamp sanity
     checking" section) can silently poison every timestamp this release's
     records carry, with no warning.
   - copy `decoder_test.go` the same way and update its imports — including
     the `TestDecodeMessage_ImplausibleTimestampFallsBackToNow` test, which
     directly verifies the line above is actually present and working

6. **Register it** in `cmd/plugin/main.go`'s `releaseHandlers` map:
   ```go
   var releaseHandlers = map[string]func(*decoder.Decoder) func([]byte) ([]decoder.Record, error){
       v6_0_0.Release: v6_0_0.NewHandler,
       v6_1_2.Release: v6_1_2.NewHandler,
       v6_2_0.Release: v6_2_0.NewHandler, // add this line
   }
   ```
   (and add the corresponding import at the top of the file)

7. **Build and test:**
   ```bash
   make test
   make build
   ```

`pkg/decoder/decoder.go` (the common package — `Decoder`, `GetTags`,
`MakeRecord`, `ProtoToFields`, `IsNilProto`, `CopyTags`) never needs to
change for a new release; it's entirely field-name/reflection based and
shared by every version.
