import { cpSync, rmSync } from 'node:fs'
import { fileURLToPath, URL } from 'node:url'

// Maven package needs current assets immediately; IDE compilers copy the registered resource root.
const source = fileURLToPath(new URL('../../target/generated-resources/console/static/', import.meta.url))
const target = fileURLToPath(new URL('../../target/classes/static/', import.meta.url))
rmSync(target, { recursive: true, force: true })
cpSync(source, target, { recursive: true })
