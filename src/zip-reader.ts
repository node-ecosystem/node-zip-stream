import { Readable } from 'node:stream'
import { createInflateRaw } from 'node:zlib'

const END_OF_CENTRAL_DIR_SIG = 0x06054B50
const CENTRAL_DIR_HEADER_SIG = 0x02014B50
const LOCAL_FILE_HEADER_SIG = 0x04034B50

const METHOD_STORED = 0
const METHOD_DEFLATE = 8

export interface ZipReadEntry {
  name: string
  compressedSize: number
  uncompressedSize: number
  localHeaderOffset: number
  method: number
}

const findEndOfCentralDirectory = (buffer: Buffer): number => {
  // EOCD is at the end of the file; the comment (usually empty) can
  // extend up to 65535 bytes, so we search backwards.
  const minOffset = Math.max(0, buffer.length - 22 - 0xFFFF)

  for (let i = buffer.length - 22; i >= minOffset; i--) {
    if (buffer.readUInt32LE(i) === END_OF_CENTRAL_DIR_SIG) {
      return i
    }
  }

  throw new Error('Invalid ZIP file: End Of Central Directory not found')
}

/**
 * Reads the central directory of a ZIP file (already in memory as
 * a Buffer, e.g. an upload) and returns the list of entries with
 * offset/size, without decompressing anything.
 *
 * Limitation: no ZIP64 support (consistent with the writer used for the
 * backup) → not suitable for single entries/archives over ~4GB.
 */
export const readZipEntries = (buffer: Buffer): ZipReadEntry[] => {
  const eocdOffset = findEndOfCentralDirectory(buffer)
  const totalEntries = buffer.readUInt16LE(eocdOffset + 10)
  const centralDirOffset = buffer.readUInt32LE(eocdOffset + 16)

  const entries: ZipReadEntry[] = []
  let offset = centralDirOffset

  for (let i = 0; i < totalEntries; i++) {
    if (buffer.readUInt32LE(offset) !== CENTRAL_DIR_HEADER_SIG) {
      throw new Error('Invalid ZIP file: Central Directory Header not valid')
    }

    const method = buffer.readUInt16LE(offset + 10)
    const compressedSize = buffer.readUInt32LE(offset + 20)
    const uncompressedSize = buffer.readUInt32LE(offset + 24)
    const nameLength = buffer.readUInt16LE(offset + 28)
    const extraLength = buffer.readUInt16LE(offset + 30)
    const commentLength = buffer.readUInt16LE(offset + 32)
    const localHeaderOffset = buffer.readUInt32LE(offset + 42)

    const name = buffer
      .subarray(offset + 46, offset + 46 + nameLength)
      .toString('utf8')

    entries.push({ name, compressedSize, uncompressedSize, localHeaderOffset, method })

    offset += 46 + nameLength + extraLength + commentLength
  }

  return entries
}

/**
 * Opens a read stream for an entry, decompressing it on the fly (DEFLATE)
 * or passing it as-is (STORED). Reads the compressed data directly from
 * the original buffer, without extracting anything to disk or duplicating
 * the entire content in memory.
 */
export const openZipEntryStream = (buffer: Buffer, entry: ZipReadEntry): Readable => {
  if (buffer.readUInt32LE(entry.localHeaderOffset) !== LOCAL_FILE_HEADER_SIG) {
    throw new Error(`Invalid ZIP file: Local File Header not valid for "${entry.name}"`)
  }

  const nameLength = buffer.readUInt16LE(entry.localHeaderOffset + 26)
  const extraLength = buffer.readUInt16LE(entry.localHeaderOffset + 28)
  const dataStart = entry.localHeaderOffset + 30 + nameLength + extraLength
  const compressedData = buffer.subarray(dataStart, dataStart + entry.compressedSize)

  if (entry.method === METHOD_STORED) {
    return Readable.from(compressedData)
  }

  if (entry.method === METHOD_DEFLATE) {
    const inflate = createInflateRaw()
    inflate.end(compressedData)
    return inflate
  }

  throw new Error(`Unsupported compression method (${entry.method}) for "${entry.name}"`)
}
