# node-zip-stream

Minimal, dependency-free ZIP reader/writer for Node.js, built entirely on `node:zlib` and `node:stream`.

Designed for use cases where you need to **produce or consume ZIP archives without ever buffering the whole content in memory** — e.g. streaming a database export straight into an HTTP response, or reading an uploaded ZIP file entry-by-entry without extracting it to disk first.
Why

Most ZIP libraries for Node.js ([adm-zip](https://github.com/cthackers/adm-zip), [jszip](https://github.com/Stuk/jszip), [archiver](https://github.com/archiverjs/node-archiver) etc) buffer each entry — and often the whole archive — in memory before writing or after reading. That works fine for small files, but doesn't scale: if an entry is a multi-gigabyte export, you'll run out of memory before you ever touch the disk or the network.

This package instead:
- **Writes** ZIP archives incrementally, compressing each entry with `zlib.createDeflateRaw()` as data flows in from a source stream, using the ZIP data descriptor mechanism to write CRC32/size **after** the compressed data (since they're only known once the stream is fully consumed).
- **Reads** ZIP archives by parsing only the central directory (a few KB at the end of the file) and decompressing each entry on demand with `zlib.createInflateRaw()`, without extracting anything to disk.

Zero runtime dependencies — everything is built on Node.js core modules (`node:zlib`, `node:stream`).

### Requirements
- Node.js >= 20.15.0 or >= 22.2.0 (uses `zlib.crc32`)

### Installation

```sh
# npm
npm install node-zip-stream

# pnpm
pnpm add node-zip-stream

# yarn
yarn add node-zip-stream
```

### Limitations

- **No ZIP64 support**: individual entries and the overall archive are limited to ~4GB (uncompressed and compressed size fields are 32-bit). Adding ZIP64 extra fields is possible but not implemented here.
- **No encryption**, no split archives, no Unicode extra fields — this is a minimal implementation covering the common case (DEFLATE and STORED methods, standard local/central directory headers).
- The writer always uses the **data descriptor** flag (bit 3), which is supported by all mainstream ZIP tools (7-Zip, Info-ZIP, Windows Explorer, macOS Archive Utility) but may not be supported by very old or non-compliant readers.
- The reader expects the uploaded/loaded ZIP to already be fully available as a Buffer in memory (e.g. after receiving a file upload). Streaming the __reading of the ZIP bytes themselves__ from an incoming HTTP request is out of scope — only decompression of each entry's content is streamed.

### Usage
#### Writing a ZIP archive on the fly

`createZipStream` takes a list of entries, each backed by a `Readable`, and yields `Buffer` chunks forming a valid ZIP file. It never waits for an entry to finish before starting to emit compressed bytes.

```ts
import { createWriteStream } from 'node:fs'
import { Readable } from 'node:stream'
import { createZipStream, type ZipEntry } from 'node-zip-stream/write'

const entries: ZipEntry[] = [
  {
    name: 'hello.txt',
    stream: Readable.from(['Hello, ', 'world!'])
  },
  {
    name: 'data.json',
    stream: Readable.from(JSON.stringify({ ok: true }))
  }
]

const output = createWriteStream('archive.zip')

for await (const chunk of createZipStream(entries)) {
  output.write(chunk)
}

output.end()
```

#### Streaming a large data source into a ZIP entry

Any `Readable` works as an entry source, so you can plug in a database cursor, a large file, or any generator without ever materializing the full content in memory:

```ts
import { Readable } from 'node:stream'
import { createZipStream, type ZipEntry } from 'node-zip-stream/write'

async function* generateRows() {
  for (let i = 0; i < 1_000_000; i++) {
    yield Buffer.from(`row-${i}\n`)
  }
}

const entries: ZipEntry[] = [
  { name: 'rows.txt', stream: Readable.from(generateRows()) }
]

for await (const chunk of createZipStream(entries)) {
  // e.g. write to an HTTP response, a file, a socket...
}
```

#### Piping directly into an HTTP response (Hono example)

```ts
import { Readable } from 'node:stream'
import type { Context } from 'hono'
import { stream as honoStream } from 'hono/streaming'
import { createZipStream, type ZipEntry } from 'node-zip-stream/write'

export default async function download(c: Context) {
  const entries: ZipEntry[] = [
    { name: 'export.json', stream: Readable.from(getDataAsJsonChunks()) }
  ]

  c.header('Content-Disposition', 'attachment; filename="export.zip"')
  c.header('Content-Type', 'application/zip')

  return honoStream(c, async (s) => {
    for await (const chunk of createZipStream(entries)) {
      await s.write(chunk)
    }
  })
}
```

#### Reading a ZIP archive
`readZipEntries` parses the central directory of an in-memory `Buffer` and returns metadata for each entry (name, offsets, sizes, compression method) without decompressing anything. `openZipEntryStream` then opens a decompression stream for a single entry on demand.

```ts
import { readZipEntries, openZipEntryStream } from 'node-zip-stream/read'

const buffer = await fs.promises.readFile('archive.zip')

const entries = readZipEntries(buffer)

for (const entry of entries) {
  console.log(entry.name, entry.uncompressedSize)

  const contentStream = openZipEntryStream(buffer, entry)

  for await (const chunk of contentStream) {
    // process the decompressed content incrementally
  }
}
```

#### Processing entries in parallel

Since each entry only reads its own slice of the source buffer, entries can safely be processed concurrently:

```ts
import { readZipEntries, openZipEntryStream } from 'node-zip-stream/read'

const buffer = await getUploadedZipBuffer()
const entries = readZipEntries(buffer)

await Promise.all(
  entries.map(async (entry) => {
    const stream = openZipEntryStream(buffer, entry)
    await processEntryStream(entry.name, stream)
  })
)
```

### API
`streaming-zip/write`
`createZipStream(entries: ZipEntry[]): AsyncGenerator<Buffer>`

Produces a valid ZIP file as a sequence of `Buffer` chunks. Entries are processed sequentially, in the order provided; each entry's source stream is fully consumed (and compressed via DEFLATE) before moving to the next.

```ts
interface ZipEntry {
  name: string       // entry path/name inside the archive
  stream: Readable   // source of the (uncompressed) entry content
}
```

`streaming-zip/read`
`readZipEntries(buffer: Buffer): ZipReadEntry[]`

Parses the ZIP central directory and returns entry metadata. Does not read or decompress entry content.

```ts
interface ZipReadEntry {
  name: string
  compressedSize: number
  uncompressedSize: number
  localHeaderOffset: number
  method: number   // 0 = stored, 8 = deflate
}
```

`openZipEntryStream(buffer: Buffer, entry: ZipReadEntry): Readable`

Returns a `Readable` stream that emits the decompressed content of the given entry. Supports `STORED` (method `0`) and `DEFLATE` (method `8`); throws for any other compression method.

### Format compatibility

Archives produced by `createZipStream` can be opened by any standard ZIP tool (7-Zip, Info-ZIP `unzip`, Windows Explorer, macOS Archive Utility, AdmZip, etc.), and can of course be read back by `readZipEntries`/`openZipEntryStream`.

Conversely, `readZipEntries`/`openZipEntryStream` can read standard ZIP archives produced by other tools, as long as they don't rely on ZIP64 or encryption (see [Limitations](#limitations)).

### 📜 License

[MIT](LICENSE)
