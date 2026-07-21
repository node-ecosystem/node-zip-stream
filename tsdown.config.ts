import type { UserConfig } from 'tsdown'

export default {
  entry: {
    'read': 'src/zip-reader.ts',
    'write': 'src/zip-stream.ts'
  }
} satisfies UserConfig
