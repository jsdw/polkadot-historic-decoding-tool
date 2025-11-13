use std::io::Write;

use crate::utils;
use crate::utils::runner::RoundRobin;
use anyhow::{anyhow, Context};
use clap::Parser;
use parity_scale_codec::Encode;
use subxt::ext::codec::Decode;
use subxt::{Config, PolkadotConfig};
use subxt_rpcs::{
    client::{rpc_params, RpcClient},
    methods::legacy::{Bytes, LegacyRpcMethods, NumberOrHex},
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Opts {
    /// URL of the node to connect to.
    /// Defaults to using Polkadot RPC URLs if not given.
    #[arg(short, long)]
    pub url: Option<String>,

    /// Block number to fetch metadata from.
    #[arg(short, long)]
    pub block: u64,

    /// As binary?
    #[arg(long)]
    pub binary: bool,

    /// Save to a file instead of stdout
    #[arg(short, long)]
    pub output: Option<std::path::PathBuf>,
}

pub async fn run(opts: Opts) -> anyhow::Result<()> {
    let block_number = opts.block;
    let as_binary = opts.binary;

    let metadata = fetch_metadata(opts.url, block_number).await?;

    let mut writer: Box<dyn std::io::Write> = match opts.output {
        None => Box::new(std::io::stdout()),
        Some(file_path) => Box::new(std::fs::File::create(file_path)?),
    };

    if as_binary {
        let encoded = metadata.encode();
        writer.write_all(&encoded)?;
    } else {
        serde_json::to_writer_pretty(writer, &metadata)?;
    }
    Ok(())
}

pub(super) async fn fetch_metadata(url: Option<String>, block_number: u64) -> anyhow::Result<frame_metadata::RuntimeMetadata> {
    // Use our the given URl, or polkadot RPC node urls if not given.
    let urls = RoundRobin::new(utils::url_or_polkadot_rpc_nodes(url.as_deref()));

    let url = urls.get();
    let rpc_client = RpcClient::from_insecure_url(url).await?;
    let rpcs = LegacyRpcMethods::<PolkadotConfig>::new(rpc_client.clone());
    let block_hash = rpcs
        .chain_get_block_hash(Some(NumberOrHex::Number(block_number as u64)))
        .await
        .with_context(|| "Could not fetch block hash")?
        .ok_or_else(|| anyhow!("Couldn't find block {block_number}"))?;
    let metadata = state_get_metadata(&rpc_client, Some(block_hash))
        .await
        .with_context(|| "Could not fetch metadata")?;

    Ok(metadata)
}

pub(super) async fn state_get_metadata(
    client: &RpcClient,
    at: Option<<PolkadotConfig as Config>::Hash>,
) -> anyhow::Result<frame_metadata::RuntimeMetadata> {
    let bytes: Bytes = client
        .request("state_getMetadata", rpc_params![at])
        .await
        .with_context(|| "Could not fetch metadata")?;
    let metadata = frame_metadata::RuntimeMetadataPrefixed::decode(&mut &bytes[..])
        .with_context(|| "Could not decode metadata")?;
    Ok(metadata.1)
}
