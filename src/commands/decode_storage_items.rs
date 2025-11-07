use self::skip::SkipDecoding;
use super::fetch_metadata::state_get_metadata;
use super::find_spec_changes::SpecVersionUpdate;
use crate::decoding::storage_decoder;
use crate::decoding::storage_decoder::{write_storage_keys, StorageKey};
use crate::utils::{
    self,
    runner::{RoundRobin, Runner},
};
use crate::utils::{write_value, IndentedWriter};
use anyhow::{anyhow, Context};
use clap::Parser;
use frame_decode::storage::StorageHasher;
use frame_metadata::RuntimeMetadata;
use scale_info_legacy::ChainTypeRegistry;
use std::collections::VecDeque;
use std::io::Write as _;
use std::sync::Arc;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};
use subxt::{
    backend::{
        legacy::{rpc_methods::Bytes, LegacyBackend, LegacyRpcMethods},
        rpc::RpcClient,
        Backend,
    },
    utils::H256,
    PolkadotConfig,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Opts {
    /// Historic type definitions.
    #[arg(short, long)]
    types: PathBuf,

    /// Spec version updates.
    #[arg(short, long)]
    spec_versions: Option<PathBuf>,

    /// URL of the node to connect to.
    /// Defaults to using Polkadot RPC URLs if not given.
    #[arg(short, long)]
    url: Option<String>,

    /// How many storage decode tasks/connections to run in parallel.
    #[arg(long)]
    connections: Option<usize>,

    /// Only log errors; don't log extrinsics that decode successfully.
    #[arg(short, long)]
    errors_only: bool,

    /// Keep outputting blocks once we hit an error.
    #[arg(long)]
    continue_on_error: bool,

    /// The seed to start from. Blocks are picked in a deterministic way,
    /// and so we can provide this to continue from where we left off.
    #[arg(long)]
    starting_number: Option<usize>,

    /// The starting entry eg Staking.ActiveEra. We'll begin from this on
    /// our initial block.
    #[arg(long)]
    starting_entry: Option<StartingEntry>,

    /// The max number of storage items to download for a given storage map.
    /// Defaults to downloading all of them.
    #[arg(long, default_value = "0")]
    max_storage_entries: usize,

    /// Print the hex encoded storage key/value bytes too.
    #[arg(long)]
    print_bytes: bool,
}

pub async fn run(opts: Opts) -> anyhow::Result<()> {
    let connections = opts.connections.unwrap_or(1);
    let starting_number = opts.starting_number.unwrap_or(0);
    let mut starting_entry = opts.starting_entry;
    let urls = Arc::new(RoundRobin::new(utils::url_or_polkadot_rpc_nodes(
        opts.url.as_deref(),
    )));
    let errors_only = opts.errors_only;
    let continue_on_error = opts.continue_on_error;
    let max_storage_entries = opts.max_storage_entries;
    let print_bytes = opts.print_bytes;

    let historic_types: Arc<ChainTypeRegistry> = Arc::new({
        let historic_types_str = std::fs::read_to_string(&opts.types)
            .with_context(|| "Could not load historic types")?;
        serde_yaml::from_str(&historic_types_str)
            .with_context(|| "Can't parse historic types from JSON")?
    });
    let spec_versions = opts
        .spec_versions
        .as_ref()
        .map(|path| {
            let spec_versions_str =
                std::fs::read_to_string(path).with_context(|| "Could not load spec versions")?;
            serde_json::from_str::<Vec<SpecVersionUpdate>>(&spec_versions_str)
                .with_context(|| "Could not parse spec version JSON")
        })
        .transpose()?;

    let mut number = starting_number;
    'outer: loop {
        // In the outer loop we select a block.
        let spec_versions = spec_versions.as_ref().map(|s| s.as_slice());
        let block_number = pick_pseudorandom_block(spec_versions, number);
        let runtime_update_block_number = block_number.saturating_sub(1);

        loop {
            // In the inner loop we connect to a client and try to download entries.
            // If we hit a recoverable error, restart this loop to try again.
            let url = urls.get();
            let rpc_client = match RpcClient::from_insecure_url(url).await {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("Couldn't instantiate RPC client: {e}");
                    continue;
                }
            };
            let rpcs = LegacyRpcMethods::<PolkadotConfig>::new(rpc_client.clone());

            let runtime_update_block_hash = match rpcs
                .chain_get_block_hash(Some(runtime_update_block_number.into()))
                .await
            {
                Ok(Some(hash)) => hash,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Couldn't get block hash for {block_number}; will try again: {e}");
                    continue;
                }
            };
            let block_hash = match rpcs.chain_get_block_hash(Some(block_number.into())).await {
                Ok(Some(hash)) => hash,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Couldn't get block hash for {block_number}; will try again: {e}");
                    continue;
                }
            };
            let metadata =
                match state_get_metadata(&rpc_client, Some(runtime_update_block_hash)).await {
                    Ok(metadata) => Arc::new(metadata),
                    Err(e) => {
                        eprintln!("Couldn't get metadata at {block_number}; will try again: {e}");
                        continue;
                    }
                };
            let runtime_version = match rpcs
                .state_get_runtime_version(Some(runtime_update_block_hash))
                .await
            {
                Ok(runtime_version) => runtime_version,
                Err(e) => {
                    eprintln!(
                        "Couldn't get runtime version at {block_number}; will try again: {e}"
                    );
                    continue;
                }
            };
            let storage_entries: VecDeque<_> = {
                let entries = frame_decode::helpers::list_storage_entries_any(&metadata);
                match starting_entry {
                    None => entries.map(|e| e.into_owned()).collect(),
                    Some(se) => {
                        let se_pallet = se.pallet.to_ascii_lowercase();
                        let se_entry = se.entry.to_ascii_lowercase();
                        starting_entry = None;

                        entries
                            .skip_while(|e| {
                                e.pallet().to_ascii_lowercase() != se_pallet
                                    || e.entry().to_ascii_lowercase() != se_entry
                            })
                            .map(|e| e.into_owned())
                            .collect()
                    }
                }
            };

            // Print header for block.
            {
                let mut stdout = std::io::stdout().lock();
                writeln!(stdout, "==============================================")?;
                writeln!(stdout, "Number {number}")?;
                writeln!(
                    stdout,
                    "Storage for block {block_number} ({})",
                    subxt::utils::to_hex(block_hash)
                )?;
                writeln!(stdout, "Spec version {}", runtime_version.spec_version)?;
            }

            let stop = Arc::new(AtomicBool::new(false));
            let stop2 = stop.clone();

            // try to decode storage entries in parallel.
            let runner =
                Runner::new(
                    (
                        block_hash,
                        storage_entries,
                        urls.clone(),
                        historic_types.clone(),
                        metadata,
                        runtime_version.spec_version,
                    ),
                    // Connect to an RPC client to start decoding storage entries
                    |_task_idx,
                     (
                        block_hash,
                        storage_entries,
                        urls,
                        historic_types,
                        metadata,
                        spec_version,
                    )| {
                        let url = urls.get().clone();
                        let storage_entries = storage_entries.clone();
                        let block_hash = *block_hash;
                        let historic_types = historic_types.clone();
                        let metadata = metadata.clone();
                        let spec_version = *spec_version;
                        let skipper = SkipDecoding::new();

                        async move {
                            let rpc_client = RpcClient::from_insecure_url(url).await?;
                            let backend = LegacyBackend::builder()
                                .storage_page_size(128)
                                .build(rpc_client);

                            Ok(Some(Arc::new(RunnerState {
                                backend,
                                block_hash,
                                storage_entries,
                                historic_types,
                                metadata,
                                spec_version,
                                skipper,
                            })))
                        }
                    },
                    // Based on task number, decode an entry from the list, returning None when number exceeds list length.
                    move |task_num, state| {
                        let state = state.clone();

                        async move {
                            let Some(storage_entry) = state.storage_entries.get(task_num as usize)
                            else {
                                return Ok(None);
                            };
                            let metadata = &state.metadata;
                            let mut historic_types_for_spec = state
                                .historic_types
                                .for_spec_version(state.spec_version as u64)
                                .to_owned();

                            let metadata_types =
                                frame_decode::helpers::type_registry_from_metadata_any(&metadata)?;
                            historic_types_for_spec.prepend(metadata_types);

                            let pallet = storage_entry.pallet();
                            let entry = storage_entry.entry();
                            let at = state.block_hash;
                            let root_key = {
                                let mut hash = Vec::with_capacity(32);
                                hash.extend(&sp_crypto_hashing::twox_128(pallet.as_bytes()));
                                hash.extend(&sp_crypto_hashing::twox_128(entry.as_bytes()));
                                hash
                            };

                            // Iterate or fetch single value depending on entry.
                            let is_iterable = check_is_iterable(pallet, entry, &state.metadata)?;
                            let mut values = if is_iterable {
                                state.backend
                                .storage_fetch_descendant_values(root_key, at)
                                .await
                                .with_context(|| format!("Failed to get a stream of storage items for {pallet}.{entry}"))
                            } else {
                                state
                                    .backend
                                    .storage_fetch_values(vec![root_key], at)
                                    .await
                                    .with_context(|| {
                                        format!("Failed to fetch value at {pallet}.{entry}")
                                    })
                            }?;

                            let mut keyvals = vec![];

                            // Decode each value we get back.
                            let mut n = 0;
                            while let Some(value) = values.next().await {
                                if max_storage_entries > 0 && n >= max_storage_entries {
                                    break;
                                }

                                let value = match value {
                                    Ok(val) => val,
                                    // Some storage values are too big for the RPC client to download (eg exceed 10MB).
                                    // For now, this hack just ignores such errors.
                                    Err(subxt::Error::Rpc(
                                        subxt::error::RpcError::ClientError(e),
                                    )) => {
                                        let err = e.to_string();
                                        if err.contains("message too large")
                                            || err.contains("Response is too big")
                                        {
                                            let err = scale_value::Value::string(
                                                "Skipping this entry: it is too large",
                                            )
                                            .map_context(|_| "Unknown".to_string());
                                            keyvals.push(DecodedStorageKeyVal {
                                                key_bytes: Vec::new(),
                                                key: Ok(vec![StorageKey {
                                                    hash: vec![],
                                                    value: Some(err.clone()),
                                                    hasher: StorageHasher::Identity,
                                                }]),
                                                value_bytes: Vec::new(),
                                                value: Ok(err),
                                            });
                                            continue;
                                        }
                                        return Err(subxt::Error::Rpc(subxt::error::RpcError::ClientError(e)))
                                        .with_context(|| format!("Failed to get storage item in stream for {pallet}.{entry}"));
                                    }
                                    Err(e) => {
                                        return Err(e).with_context(|| format!("Failed to get storage item in stream for {pallet}.{entry}"));
                                    }
                                };

                                let key_bytes = &value.key;
                                let value_bytes = &value.value;

                                // Skip over corrupt entries.
                                if state.skipper.should_skip(state.spec_version, key_bytes) {
                                    let err = scale_value::Value::string(
                                        "Skipping this entry: it is corrupt",
                                    )
                                    .map_context(|_| "Unknown".to_string());
                                    keyvals.push(DecodedStorageKeyVal {
                                        key_bytes: Vec::new(),
                                        key: Ok(vec![StorageKey {
                                            hash: vec![],
                                            value: Some(err.clone()),
                                            hasher: StorageHasher::Identity,
                                        }]),
                                        value_bytes: value_bytes.clone(),
                                        value: Ok(err),
                                    });
                                    continue;
                                }

                                let key = storage_decoder::decode_storage_keys(
                                    pallet,
                                    entry,
                                    key_bytes,
                                    metadata,
                                    &historic_types_for_spec,
                                )
                                .with_context(|| {
                                    format!("Failed to decode storage key in {pallet}.{entry}")
                                });
                                let value = storage_decoder::decode_storage_value(
                                    pallet,
                                    entry,
                                    &value.value,
                                    metadata,
                                    &historic_types_for_spec,
                                )
                                .with_context(|| {
                                    format!("Failed to decode storage value in {pallet}.{entry}")
                                });

                                keyvals.push(DecodedStorageKeyVal {
                                    key_bytes: key_bytes.clone(),
                                    key,
                                    value_bytes: value_bytes.clone(),
                                    value,
                                });

                                n += 1;
                            }

                            Ok(Some(DecodedStorageEntry {
                                pallet: pallet.to_string(),
                                entry: entry.to_string(),
                                keyvals,
                            }))
                        }
                    },
                    // Output details.
                    move |output| {
                        if output.keyvals.is_empty() {
                            return Ok(());
                        }

                        let mut stdout = std::io::stdout().lock();

                        let is_error = output
                            .keyvals
                            .iter()
                            .any(|kv| kv.key.is_err() || kv.value.is_err());
                        let should_print_header = !errors_only || (errors_only && is_error);
                        let should_print_success = !errors_only;

                        if should_print_header {
                            writeln!(
                                stdout,
                                "\n{}.{} (b:{block_number}, n:{number})",
                                output.pallet, output.entry
                            )?;
                        }

                        if print_bytes {
                            let out = output
                                .keyvals
                                .iter()
                                .map(|kv| {
                                    (Bytes(kv.key_bytes.clone()), Bytes(kv.value_bytes.clone()))
                                })
                                .collect::<Vec<_>>();
                            let out_str = serde_json::to_string_pretty(&out).unwrap();
                            writeln!(stdout, "  Keyvals hex: {out_str}")?;
                        }

                        for (
                            idx,
                            DecodedStorageKeyVal {
                                key_bytes: _,
                                key,
                                value_bytes: _,
                                value,
                            },
                        ) in output.keyvals.iter().enumerate()
                        {
                            if key.is_ok() && value.is_ok() && !should_print_success {
                                continue;
                            }

                            write!(stdout, "  [{idx}] ")?;
                            match &key {
                                Ok(key) => {
                                    write_storage_keys(IndentedWriter::<2, _>(&mut stdout), key)?;
                                }
                                Err(e) => {
                                    write!(
                                        IndentedWriter::<2, _>(&mut stdout),
                                        "Key Error (block {block_number}, number {number}): {e:?}"
                                    )?;
                                }
                            }
                            write!(stdout, "\n    - ")?;
                            match &value {
                                Ok(value) => {
                                    write_value(IndentedWriter::<6, _>(&mut stdout), value)?;
                                }
                                Err(e) => {
                                    write!(IndentedWriter::<6, _>(&mut stdout), "Value Error (block {block_number}, number {number}): {e:?}")?;
                                }
                            }
                            writeln!(stdout)?;

                            let is_this_error = key.is_err() || value.is_err();
                            if is_this_error && !continue_on_error {
                                break;
                            }
                        }

                        if !continue_on_error && is_error {
                            stop2.store(true, Ordering::Relaxed);
                            Err(anyhow!("Stopping: error decoding storage entries."))
                        } else {
                            Ok(())
                        }
                    },
                );

            // Decode storage entries in the block.
            let _ = runner.run(connections, 0).await;
            // Stop if the runner tells us to. Quite a hacky way to communicate it.
            if stop.load(Ordering::Relaxed) == true {
                break 'outer;
            }
            // Don't retry this block; move on to next.
            break;
        }

        number += 1;
    }

    Ok(())
}

/// This allows us to skip decoding entries that are corrupt or otherwise undecodeable.
mod skip {
    pub struct SkipDecoding(Vec<(Vec<u8>, u32)>);

    impl SkipDecoding {
        /// This defines the hardcoded items to skip.
        pub fn new() -> Self {
            SkipDecoding(vec![
                (
                    // Proxy.proxies has a corrupt entry in it for account ID 0x0E6DE68B13B82479FBE988AB9ECB16BAD446B67B993CDD9198CD41C7C6259C49:
                    hex::decode("1809d78346727a0ef58c0fa03bafa3231d885dcfb277f185f2d8e62a5f290c854d2d16b4be62d0e00e6de68b13b82479fbe988ab9ecb16bad446b67b993cdd9198cd41c7c6259c49").unwrap(),
                    // spec version it becomes a problem:
                    23
                )
            ])
        }

        /// Should we skip some entry.
        pub fn should_skip(&self, spec_version: u32, key: &[u8]) -> bool {
            self.0
                .iter()
                .find(|(skip_key, skip_spec)| *skip_key == key && *skip_spec <= spec_version)
                .is_some()
        }
    }
}

/// Is this storage entry iterable? If so, we'll iterate it. If not, we can just retrieve the single entry.
pub fn check_is_iterable(
    pallet_name: &str,
    storage_entry: &str,
    metadata: &RuntimeMetadata,
) -> anyhow::Result<bool> {
    fn inner<Info: frame_decode::storage::StorageTypeInfo>(
        pallet_name: &str,
        storage_entry: &str,
        info: &Info,
    ) -> anyhow::Result<bool> {
        let storage_info = info
            .get_storage_info(pallet_name, storage_entry)
            .map_err(|e| e.into_owned())?;
        let is_empty = storage_info.keys.is_empty();
        Ok(!is_empty)
    }

    match metadata {
        RuntimeMetadata::V8(m) => inner(pallet_name, storage_entry, m),
        RuntimeMetadata::V9(m) => inner(pallet_name, storage_entry, m),
        RuntimeMetadata::V10(m) => inner(pallet_name, storage_entry, m),
        RuntimeMetadata::V11(m) => inner(pallet_name, storage_entry, m),
        RuntimeMetadata::V12(m) => inner(pallet_name, storage_entry, m),
        RuntimeMetadata::V13(m) => inner(pallet_name, storage_entry, m),
        RuntimeMetadata::V14(m) => inner(pallet_name, storage_entry, m),
        RuntimeMetadata::V15(m) => inner(pallet_name, storage_entry, m),
        _ => anyhow::bail!("Only metadata V8 - V15 is supported"),
    }
}

/// Given the same spec versions and the same number, this should output the same value,
/// but the output block number can be pseudorandom in nature. The output number should be
/// between the first and last spec versions provided (so blocks newer than the last runtime
/// upgrade aren't tested).
fn pick_pseudorandom_block(spec_versions: Option<&[SpecVersionUpdate]>, number: usize) -> u64 {
    let Some(spec_versions) = spec_versions else {
        return number as u64;
    };

    // Given spec versions, we deterministically work from first blocks seen (ie blocks before
    // update is enacted, which is a good edge to test) and then blocks after and so on.
    // 0 0 0 1 1 1 2 2 2 3 3
    // 0 4   1 5   2 6   3 7
    let spec_version_idx = number % spec_versions.len();
    let spec_version_block_idx = (number / spec_versions.len()) * 1001; // move 1001 blocks forward each time to sample more range

    let block_number = spec_versions[spec_version_idx].block + spec_version_block_idx as u64;
    block_number
}

struct RunnerState {
    backend: LegacyBackend<PolkadotConfig>,
    block_hash: H256,
    storage_entries: VecDeque<frame_decode::helpers::StorageEntry<'static>>,
    historic_types: Arc<ChainTypeRegistry>,
    metadata: Arc<RuntimeMetadata>,
    spec_version: u32,
    skipper: SkipDecoding,
}

struct DecodedStorageEntry {
    pallet: String,
    entry: String,
    keyvals: Vec<DecodedStorageKeyVal>,
}

struct DecodedStorageKeyVal {
    // For debugging we make the key bytes available in the output, but don't need them normally.
    key_bytes: Vec<u8>,
    key: anyhow::Result<Vec<StorageKey>>,
    value_bytes: Vec<u8>,
    value: anyhow::Result<scale_value::Value<String>>,
}

#[derive(Clone)]
struct StartingEntry {
    pallet: String,
    entry: String,
}

impl std::str::FromStr for StartingEntry {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(".");
        let pallet = parts.next().ok_or_else(|| {
            anyhow!("starting entry should take the form $pallet.$name, but no $pallet found")
        })?;
        let entry = parts.next().ok_or_else(|| {
            anyhow!("starting entry should take the form $pallet.$name, but no $name found")
        })?;
        Ok(StartingEntry {
            pallet: pallet.to_string(),
            entry: entry.to_string(),
        })
    }
}
