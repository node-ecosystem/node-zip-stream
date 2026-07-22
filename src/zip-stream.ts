import type { Readable } from 'node:stream'
import { createDeflateRaw, crc32 } from 'node:zlib'

const LOCAL_FILE_HEADER_SIG = 0x04034B50
const DATA_DESCRIPTOR_SIG = 0x08074B50
const CENTRAL_DIR_HEADER_SIG = 0x02014B50
const END_OF_CENTRAL_DIR_SIG = 0x06054B50

const FLAG_DATA_DESCRIPTOR = 0x0008 // crc/size written AFTER the compressed data
const METHOD_DEFLATE = 8

export interface ZipEntry {
  name: string
  stream: Readable
}

const toDosDateTime = (date: Date): { time: number; date: number } => {
  const time = (date.getHours() << 11) | (date.getMinutes() << 5) | (date.getSeconds() >> 1)
  const dosDate = ((date.getFullYear() - 1980) << 9) | ((date.getMonth() + 1) << 5) | date.getDate()
  return { time, date: dosDate }
}

const buildLocalFileHeader = (nameBuf: Buffer, dosTime: number, dosDate: number): Buffer => {
  const header = Buffer.alloc(30 + nameBuf.length)

  header.writeUInt32LE(LOCAL_FILE_HEADER_SIG, 0)
  header.writeUInt16LE(20, 4) // version needed to extract
  header.writeUInt16LE(FLAG_DATA_DESCRIPTOR, 6)
  header.writeUInt16LE(METHOD_DEFLATE, 8)
  header.writeUInt16LE(dosTime, 10)
  header.writeUInt16LE(dosDate, 12)
  header.writeUInt32LE(0, 14) // crc32 (unknown, will be in data descriptor)
  header.writeUInt32LE(0, 18) // compressed size (unknown)
  header.writeUInt32LE(0, 22) // uncompressed size (unknown)
  header.writeUInt16LE(nameBuf.length, 26)
  header.writeUInt16LE(0, 28) // extra field length
  nameBuf.copy(header, 30)

  return header
}

const buildDataDescriptor = (
  crc: number,
  compressedSize: number,
  uncompressedSize: number
): Buffer => {
  const descriptor = Buffer.alloc(16)

  descriptor.writeUInt32LE(DATA_DESCRIPTOR_SIG, 0)
  descriptor.writeUInt32LE(crc, 4)
  descriptor.writeUInt32LE(compressedSize, 8)
  descriptor.writeUInt32LE(uncompressedSize, 12)

  return descriptor
}

const buildCentralDirectoryHeader = (
  nameBuf: Buffer,
  crc: number,
  compressedSize: number,
  uncompressedSize: number,
  localHeaderOffset: number,
  dosTime: number,
  dosDate: number
): Buffer => {
  const header = Buffer.alloc(46 + nameBuf.length)

  header.writeUInt32LE(CENTRAL_DIR_HEADER_SIG, 0)
  header.writeUInt16LE(20, 4) // version made by
  header.writeUInt16LE(20, 6) // version needed to extract
  header.writeUInt16LE(FLAG_DATA_DESCRIPTOR, 8)
  header.writeUInt16LE(METHOD_DEFLATE, 10)
  header.writeUInt16LE(dosTime, 12)
  header.writeUInt16LE(dosDate, 14)
  header.writeUInt32LE(crc, 16)
  header.writeUInt32LE(compressedSize, 20)
  header.writeUInt32LE(uncompressedSize, 24)
  header.writeUInt16LE(nameBuf.length, 28)
  header.writeUInt16LE(0, 30) // extra field length
  header.writeUInt16LE(0, 32) // comment length
  header.writeUInt16LE(0, 34) // disk number start
  header.writeUInt16LE(0, 36) // internal attrs
  header.writeUInt32LE(0, 38) // external attrs
  header.writeUInt32LE(localHeaderOffset, 42)
  nameBuf.copy(header, 46)

  return header
}

const buildEndOfCentralDirectory = (
  totalEntries: number,
  centralDirSize: number,
  centralDirOffset: number
): Buffer => {
  const eocd = Buffer.alloc(22)

  eocd.writeUInt32LE(END_OF_CENTRAL_DIR_SIG, 0)
  eocd.writeUInt16LE(0, 4) // current disk number
  eocd.writeUInt16LE(0, 6) // disk with start of central dir
  eocd.writeUInt16LE(totalEntries, 8)
  eocd.writeUInt16LE(totalEntries, 10)
  eocd.writeUInt32LE(centralDirSize, 12)
  eocd.writeUInt32LE(centralDirOffset, 16)
  eocd.writeUInt16LE(0, 20) // comment length

  return eocd
}

/**
 * Generates a valid ZIP file on-the-fly, entry by entry, without ever
 * buffering the entire content in memory: each entry is
 * compressed in streaming (DEFLATE) as it is read from the
 * source (e.g., MongoDB cursor) and the compressed chunks are emitted
 * immediately, using the "data descriptor" (bit 3 of the flag) to write
 * CRC32/size AFTER the data, when we finally know them.
 * 
 * Limitation: no ZIP64 support → not suitable for single entries/archives
 * over ~4GB (would require ZIP64 extra fields in the headers).
 */
export async function* createZipStream(entries: ZipEntry[]): AsyncGenerator<Uint8Array> {
  const centralDirectoryRecords: Buffer[] = []
  const { time: dosTime, date: dosDate } = toDosDateTime(new Date())

  let offset = 0

  for (const entry of entries) {
    const nameBuf = Buffer.from(entry.name, 'utf8')
    const localHeaderOffset = offset

    const localHeader = buildLocalFileHeader(nameBuf, dosTime, dosDate)
    yield localHeader
    offset += localHeader.length

    let crc = 0
    let uncompressedSize = 0
    let compressedSize = 0

    const deflate = createDeflateRaw()

    entry.stream.on('data', (chunk: Buffer) => {
      crc = crc32(chunk, crc)
      uncompressedSize += chunk.length
    })

    entry.stream.pipe(deflate)

    for await (const chunk of deflate as AsyncIterable<Buffer>) {
      compressedSize += chunk.length
      offset += chunk.length
      yield chunk
    }

    const dataDescriptor = buildDataDescriptor(crc, compressedSize, uncompressedSize)
    yield dataDescriptor
    offset += dataDescriptor.length

    centralDirectoryRecords.push(
      buildCentralDirectoryHeader(
        nameBuf,
        crc,
        compressedSize,
        uncompressedSize,
        localHeaderOffset,
        dosTime,
        dosDate
      )
    )
  }

  const centralDirOffset = offset
  let centralDirSize = 0

  for (const record of centralDirectoryRecords) {
    yield record
    centralDirSize += record.length
  }

  yield buildEndOfCentralDirectory(entries.length, centralDirSize, centralDirOffset)
}
