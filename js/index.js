#!/usr/bin/env node

import { ApiPromise, WsProvider } from "@polkadot/api"
import { exitWithError } from "./utils.js"
import argParser from "args"

const [cmd, args] = parseCommand(process.argv)

if (cmd === "decode-blocks") {
  ////
  //// Block decoding
  ////
  argParser
    .option('url', 'URL of the node to connect to')
    .option('block', 'Block number to obtain', undefined, b => parseInt(b, 10))

  const flags = argParser.parse(['node', 'index.js', ...args])
  const url = flags.url ?? "wss://polkadot-public-rpc.blockops.network/ws"
  const blockNumber = flags.block ?? exitWithError("--block number needs to be provided")

  console.log(`connecting to url: ${url}`)
  console.log(`fetching block: ${blockNumber}`)

  const wsProvider = new WsProvider(url);
  const api = await ApiPromise.create({ provider: wsProvider })
  const blockHash = await api.rpc.chain.getBlockHash(blockNumber)
  const block = await api.rpc.chain.getBlock(blockHash)

  for (const ex of block.block.extrinsics) {
    console.log(`${ex.method.section}.${ex.method.method}`)
    console.log(JSON.stringify(ex.method, undefined, "\t"))
  }

} else if (cmd === "decode-storage-items") {
  ////
  //// Storage item decoding
  ////
  argParser
    .option('url', 'URL of the node to connect to')
    .option('block', 'Block to decode storage from', undefined, b => parseInt(b, 10))
    .option('entry', 'Storage entry to download')
    .option('print-bytes', 'Print bytes too')

  const flags = argParser.parse(['node', 'index.js', ...args])
  const url = flags.url ?? "wss://polkadot-public-rpc.blockops.network/ws"
  const blockNumber = flags.block ?? exitWithError("--block number needs to be provided")
  const storageEntry = flags.entry ?? exitWithError("--entry pallet.name needs to be provided")
  const printBytes = flags.printBytes

  const palletAndName = parseEntry(storageEntry)
  if (!palletAndName) {
    exitWithError("--entry should take the form pallet.name")
  }
  const [pallet, name] = palletAndName

  const wsProvider = new WsProvider(url);
  const api = await ApiPromise.create({ provider: wsProvider })
  const blockHash = await api.rpc.chain.getBlockHash(blockNumber)
  
  const apiAt = await api.at(blockHash)

  const apiPallet = apiAt.query[pallet]
  if (!apiPallet) {
    exitWithError("--entry PALLET name not found (should be camelCase)")
  }

  const apiEntry = apiPallet[name]
  if (!apiEntry) {
    exitWithError("--entry ENTRY name not found (should be camelCase)")
  }

  if (typeof apiEntry.entries === "function") {
    for (const [key, value] of await apiEntry.entries()) {
      if (!printBytes) {
        console.log(key.toHuman())
        console.log(`  ${value.toString()}`)
      } else {
        console.log(key.toHex())
        console.log(`  ${value.toHex()}`)
      }
    }
  } else {
    const result = await apiEntry()
    if (!printBytes) {
      console.log(JSON.stringify(result, null, '  '))
    } else {
      console.log(result.toHex())
    }
  }
  
} else if (cmd !== undefined) {
  ////
  //// Error: Invalid command given
  ////
  console.log(`Invalid command '${cmd}'. use 'decode-blocks' or 'decode-storage-items'`)
  process.exit(1)

} else {
  ////
  //// Error: No command given
  ////
  console.log(`No command given. use 'decode-blocks' or 'decode-storage-items'`)
  process.exit(1)

}

// The quickest way to end the process; otherwise PJS keeps it open.
process.exit(0)

/**
 * Parse the top level command from the args, returning the rest of the args.
 * 
 * @param {string[]} args 
 * @returns {[string | undefined, string[]]}
 */
function parseCommand(args) {
  // slice "node index.js" bit from start if needbe
  if (args.length > 2 && args[0].endsWith("node") && args[1].endsWith("index.js")) {
    args = args.slice(2)
  }

  if (args[0].startsWith("-")) {
    return [undefined, args]
  } else {
    return [args[0], args.slice(1)]
  }
}

/**
 * Parse a storage entry like Babe.Authorities into something PJS understands
 * 
 * @param {string} entry 
 * @returns {[string, string] | undefined}
 */
function parseEntry(rawEntry) {
  const [palletPart, entryPart] = rawEntry.split(".")
  if (!entryPart) {
    return undefined
  }

  const pallet = palletPart[0].toLowerCase() + palletPart.slice(1)
  const entry = entryPart[0].toLowerCase() + entryPart.slice(1)

  return [pallet, entry]
}