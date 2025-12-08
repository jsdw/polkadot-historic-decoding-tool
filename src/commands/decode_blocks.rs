use crate::decoding::extrinsic_decoder::{Extrinsic, ExtrinsicCallData, decode_extrinsic};
use crate::utils;
use crate::utils::runner::{RoundRobin, Runner};
use anyhow::{Context, anyhow};
use clap::Parser;
use frame_metadata::RuntimeMetadata;
use scale_info_legacy::{ChainTypeRegistry, TypeRegistrySet};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use subxt::{Config, PolkadotConfig};
use subxt::{
    backend::{
        legacy::{
            LegacyRpcMethods,
            rpc_methods::{Bytes, NumberOrHex},
        },
        rpc::RpcClient,
    },
    utils::H256,
};
use tokio::sync::Mutex;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Opts {
    /// Historic type definitions.
    #[arg(short, long)]
    types: PathBuf,

    /// URL of the node(s) to connect to.
    /// Defaults to using Polkadot RPC URLs if not given.
    #[arg(short, long)]
    url: Option<String>,

    /// How many connections to establish.
    #[arg(long)]
    connections: Option<usize>,

    /// Only log errors; don't log extrinsics that decode successfully.
    #[arg(short, long)]
    errors_only: bool,

    /// Keep outputting blocks once we hit an error.
    #[arg(long)]
    continue_on_error: bool,

    /// Block number to start from.
    #[arg(short, long)]
    starting_block: Option<u64>,

    /// Print the hex encoded extrinsic bytes too.
    #[arg(long)]
    print_bytes: bool,
}

pub async fn run(opts: Opts) -> anyhow::Result<()> {
    let start_block_num = opts.starting_block.unwrap_or_default();
    let errors_only = opts.errors_only;
    let continue_on_error = opts.continue_on_error;
    let print_bytes = opts.print_bytes;
    let connections = opts.connections.unwrap_or(1);
    let historic_types_str =
        std::fs::read_to_string(&opts.types).with_context(|| "Could not load historic types")?;

    // Use our default or built-in URLs if not provided.
    let urls = RoundRobin::new(utils::url_or_polkadot_rpc_nodes(opts.url.as_deref()));

    // Our base type mappings that we'll use to decode pre-V14 blocks.
    let historic_types: ChainTypeRegistry = serde_yaml::from_str(&historic_types_str)
        .with_context(|| "Can't parse historic types from JSON")?;
    let historic_types = Arc::new(historic_types);

    // Create a runner to download and decode blocks in parallel.
    let runner = Runner::new(
        // Initial state; each task fetches the next URl to connect to.
        urls,
        // Turn each URL into some state that we'll reuse to fetch a bunch of blocks. This reruns on error.
        |_n, urls| {
            let url = urls.get().to_owned();
            async move {
                let rpc_client = RpcClient::from_insecure_url(url).await?;

                let state = RunnerState {
                    rpc_client: rpc_client.clone(),
                    rpcs: LegacyRpcMethods::<PolkadotConfig>::new(rpc_client),
                    current_spec_version: u32::MAX,
                    current_metadata: None,
                    current_types_for_spec: None,
                };

                Ok(Some(Arc::new(Mutex::new(state))))
            }
        },
        // Fetch a block and decode it. This runs in parallel for number of initial state items.
        move |block_number, state| {
            let historic_types = historic_types.clone();
            let state = state.clone();
            async move {
                let mut state = state.lock().await;

                // Check the last block to see if a runtime update happened. Runtime updates
                // take effect the block after they are applied.
                let runtime_update_block = block_number.saturating_sub(1);
                let runtime_update_block_hash =
                    chain_get_block_hash(&state.rpcs, runtime_update_block)
                        .await?
                        .ok_or_else(|| anyhow!("Couldn't find block {runtime_update_block}"))?;
                let runtime_version = state.rpcs.state_get_runtime_version(Some(runtime_update_block_hash))
                    .await
                    .with_context(|| format!("Could not fetch runtime version for block {runtime_update_block} with hash {runtime_update_block_hash}"))?;

                let this_spec_version = runtime_version.spec_version;
                if this_spec_version != state.current_spec_version
                    || state.current_metadata.is_none()
                    || state.current_types_for_spec.is_none()
                {
                    // Fetch new metadata for this spec version.
                    let metadata = super::fetch_metadata::state_get_metadata(
                        &state.rpc_client,
                        Some(runtime_update_block_hash),
                    )
                    .await?;

                    // Prepare new historic type info for this new spec/metadata. Extend the type info
                    // with Call types from the metadataa so that things like utility.batch "Just Work".
                    let mut historic_types_for_spec = historic_types
                        .for_spec_version(this_spec_version as u64)
                        .to_owned();
                    let metadata_types =
                        frame_decode::helpers::type_registry_from_metadata_any(&metadata)?;
                    historic_types_for_spec.prepend(metadata_types);

                    // Print out all of the call types for any metadata we are given, for debugging etc:
                    // extrinsic_type_info::print_call_types(&historic_types_for_spec);

                    state.current_types_for_spec = Some(historic_types_for_spec);
                    state.current_metadata = Some(metadata);
                    state.current_spec_version = this_spec_version;
                }

                let current_metadata = state.current_metadata.as_ref().unwrap();
                let current_types_for_spec = state.current_types_for_spec.as_ref().unwrap();

                let Some(block_hash) = chain_get_block_hash(&state.rpcs, block_number).await?
                else {
                    return Ok(None);
                };
                let block_body = state
                    .rpcs
                    .chain_get_block(Some(block_hash))
                    .await
                    .with_context(|| "Could not fetch block body")?
                    .expect("block should exist");

                let extrinsics = block_body
                    .block
                    .extrinsics
                    .into_iter()
                    .map(|ext| {
                        let ext_bytes = &ext.0;
                        let decoded =
                            decode_extrinsic(ext_bytes, current_metadata, current_types_for_spec);
                        (ext, decoded)
                    })
                    .collect();

                Ok(Some(Output {
                    block_number,
                    block_hash,
                    spec_version: this_spec_version,
                    extrinsics,
                }))
            }
        },
        // Log the output. This runs sequentially, in order of task numbers.
        move |output: Output| {
            let mut stdout = std::io::stdout().lock();

            let block_number = output.block_number;
            let block_hash = output.block_hash;
            let spec_version = output.spec_version;
            let extrinsics = output.extrinsics;
            let is_error = extrinsics.iter().any(|(_, e)| e.is_err());
            let should_print_header = !errors_only || (errors_only && is_error);
            let should_print_success = !errors_only;

            if should_print_header {
                writeln!(stdout, "==============================================")?;
                writeln!(
                    stdout,
                    "Block {block_number} ({})",
                    subxt::utils::to_hex(block_hash)
                )?;
                writeln!(stdout, "Spec version {spec_version}")?;
            }

            if print_bytes {
                let bytes_vec: Vec<_> = extrinsics.iter().map(|ext| &ext.0).collect();
                let bytes_json = serde_json::to_string_pretty(&bytes_vec).unwrap();
                writeln!(stdout, "Extrinsic Bytes: {bytes_json}")?;
            }

            for (ext_idx, (_ext_bytes, ext_decoded)) in extrinsics.into_iter().enumerate() {
                match ext_decoded {
                    Ok(Extrinsic::Unsigned { call_data }) => {
                        if should_print_success {
                            writeln!(
                                stdout,
                                "  {}.{}:",
                                call_data.pallet_name, call_data.call_name
                            )?;
                            print_call_data(&mut stdout, &call_data)?;
                        }
                    }
                    Ok(Extrinsic::Signed {
                        address,
                        signature,
                        signed_exts,
                        call_data,
                    }) => {
                        if should_print_success {
                            writeln!(
                                stdout,
                                "  {}.{}:",
                                call_data.pallet_name, call_data.call_name
                            )?;
                            writeln!(stdout, "    Address: {address}")?;
                            writeln!(stdout, "    Signature: {signature}")?;
                            print_signed_exts(&mut stdout, &signed_exts)?;
                            print_call_data(&mut stdout, &call_data)?;
                        }
                    }
                    Ok(Extrinsic::General {
                        signed_exts,
                        call_data,
                    }) => {
                        if should_print_success {
                            writeln!(
                                stdout,
                                "  {}.{}:",
                                call_data.pallet_name, call_data.call_name
                            )?;
                            print_signed_exts(&mut stdout, &signed_exts)?;
                            print_call_data(&mut stdout, &call_data)?;
                        }
                    }
                    Err(e) => {
                        // let bytes_hex = serde_json::to_string(&ext_bytes).unwrap();
                        writeln!(stdout, "Error decoding extrinsic {ext_idx}: {e:?}")?;
                        break;
                    }
                }
            }

            if !continue_on_error && is_error {
                Err(anyhow!("Stopping: error decoding extrinsic"))
            } else {
                Ok(())
            }
        },
    );

    runner.run(connections, start_block_num).await
}

async fn chain_get_block_hash(
    rpcs: &LegacyRpcMethods<PolkadotConfig>,
    block_number: u64,
) -> anyhow::Result<Option<<PolkadotConfig as Config>::Hash>> {
    let block_hash = rpcs
        .chain_get_block_hash(Some(NumberOrHex::Number(block_number)))
        .await
        .with_context(|| "Could not fetch block hash")?;
    Ok(block_hash)
}

fn print_call_data<W: std::io::Write>(
    mut w: W,
    call_data: &ExtrinsicCallData,
) -> anyhow::Result<()> {
    writeln!(w, "    Call data:")?;
    for arg in &call_data.args {
        write!(w, "      {}: ", arg.0)?;
        utils::write_value(utils::IndentedWriter::<6, _>(&mut w), &arg.1)?;
        writeln!(w)?;
    }
    Ok(())
}

fn print_signed_exts<W: std::io::Write>(
    mut w: W,
    signed_exts: &[(String, scale_value::Value<String>)],
) -> anyhow::Result<()> {
    writeln!(w, "    Signed exts:")?;
    for ext in signed_exts {
        write!(w, "      {}: ", ext.0)?;
        utils::write_value(utils::IndentedWriter::<6, _>(&mut w), &ext.1)?;
        writeln!(w)?;
    }
    Ok(())
}

struct RunnerState {
    rpc_client: RpcClient,
    rpcs: LegacyRpcMethods<PolkadotConfig>,
    current_spec_version: u32,
    current_metadata: Option<RuntimeMetadata>,
    current_types_for_spec: Option<TypeRegistrySet<'static>>,
}

struct Output {
    spec_version: u32,
    block_number: u64,
    block_hash: H256,
    extrinsics: Vec<(Bytes, Result<Extrinsic, anyhow::Error>)>,
}
