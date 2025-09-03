# polkadot-historic-decoding-tool

This code has a few options.

The necessary type information to decode historic blocks and storage entries is found at https://github.com/paritytech/frame-decode/blob/main/types/polkadot_types.yaml, and below is simply called `polkadot_types.yaml`.

## Decoding blocks

For decoding blocks, we proceed sequentially since they are fairly fast to decode.

Example:

```
cargo run --release -- decode-blocks \
    --types polkadot_types.yaml \
    --connections 5 \
    --starting-block 1234
```

Where `connections` is the number of connections to download/decode blocks in parallel, `starting-block` is the block number to begin at, and `types` is a YAML file containing type mappings for historic Polkadot types.

## Decoding storage entries

For decoding storage entries, we select a block (iterating through one block per runtime and then moving 1001 blocks forward next time), and then decode all of the storage entries that we know about in that block.

Example:

```
cargo run --release -- decode-storage-items \
    --types polkadot_types.yaml \
    --spec-versions polkadot_old_spec_changes.json \
    --connections 5 \
    --starting-number 293 \
    --max-storage-entries 40000 \
    --starting-entry ElectionProviderMultiPhase.Snapshot
```

Where `spec-versions` is optional and is a JSON file showing where runtime updates occur (this means we can test blocks across runtimes more easily), `starting-number` is an arbitrary number that increments for each block tested and allows deterministically resuming from the same place, `starting-entry` is the storage entry to start from (useful if you hit an error and want to pick up where you left off after fixing it), `max-storage-entries` is the most entries we'll download from a storage map (defaults to all, but some take a long time because so many entries).

## Finding spec versions

You can use `cargo run --release -- find-spec-changes` to findand output a JSON file containing information about where runtime updates occur.

## Viewing metadata

You can use `cargo run --release -- fetch-metadata --block 1234` to fetch a _JSON_ formatted version of the metadata at some block.
