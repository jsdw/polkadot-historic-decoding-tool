use crate::utils;
use crate::utils::binary_chopper::{BinaryChopper, Next};
use anyhow::{anyhow, Context};
use clap::Parser;
use subxt::backend::{
    legacy::{rpc_methods::NumberOrHex, LegacyRpcMethods},
    rpc::RpcClient,
};
use subxt::PolkadotConfig;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Opts {
    /// URL of the node to connect to.
    /// Defaults to using Polkadot RPC URLs if not given.
    #[arg(short, long)]
    pub url: Option<String>,

    /// Block number to start from.
    #[arg(short, long)]
    pub starting_block: Option<u64>,

    /// Block number to end on.
    #[arg(short, long)]
    pub ending_block: Option<u64>,
}

pub async fn run(opts: Opts) -> anyhow::Result<()> {
    let spec_version_changes = get_spec_version_changes(opts).await?;
    let stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(stdout, &spec_version_changes)?;
    Ok(())
}

pub async fn get_spec_version_changes(opts: Opts) -> anyhow::Result<Vec<SpecVersionUpdate>> {
    let url = utils::url_or_polkadot_rpc_nodes(opts.url.as_deref()).remove(0);
    let rpc_client = RpcClient::from_insecure_url(&url).await?;

    let starting_block_number = opts.starting_block.unwrap_or(0);
    let latest_block_number = match opts.ending_block {
        Some(n) => n,
        None => {
            LegacyRpcMethods::<PolkadotConfig>::new(rpc_client.clone())
                .chain_get_header(None)
                .await?
                .expect("latest block will be returned when no hash given")
                .number as u64
        }
    };

    let mut low_version = get_spec_version(&rpc_client, &url, starting_block_number).await;
    let high_version = get_spec_version(&rpc_client, &url, latest_block_number).await;

    let mut start = starting_block_number;
    let end = latest_block_number;
    let mut changes = vec![];

    loop {
        let mut chopper = BinaryChopper::new((start, low_version), (end, high_version));

        // While this is true, the BinaryChopper is proposing new blocks and we are
        // providing the spec versions at them to guide it.
        while let Next::NeedsState(n) = chopper.next_value() {
            let spec_version = get_spec_version(&rpc_client, &url, n).await;
            chopper.set_state_for_next_value(spec_version);
        }

        // If no longer NeedsState, it means we're finished and have a pair of blocks
        // which have a spec version change in them.
        let ((_block_num1, spec_version1), (block_num2, spec_version2)) =
            chopper.next_value().unwrap_finished();

        // We've hit the end; if the block number provided == end, we're done.
        if block_num2 != end {
            eprintln!("Found spec version change at block {block_num2} (from spec version {spec_version1} to {spec_version2})");
            start = block_num2;
            low_version = spec_version2;
            changes.push((block_num2, spec_version2));
        } else {
            break;
        }
    }

    let updates: Vec<_> = changes
        .iter()
        .map(|&(block, spec_version)| SpecVersionUpdate {
            block,
            spec_version,
        })
        .collect();

    Ok(updates)
}

async fn get_spec_version(rpc_client: &RpcClient, url: &str, block_number: u64) -> u32 {
    retry(rpc_client.clone(), url, |rpcs: RpcClient| async move {
        let rpcs = LegacyRpcMethods::<PolkadotConfig>::new(rpcs);
        let block_hash = rpcs
            .chain_get_block_hash(Some(NumberOrHex::Number(block_number)))
            .await
            .with_context(|| format!("Could not fetch block hash for block {block_number}"))?
            .ok_or_else(|| anyhow!("Couldn't find block {block_number}"))?;
        let version = rpcs
            .state_get_runtime_version(Some(block_hash))
            .await
            .with_context(|| "Could not fetch runtime version")?;
        Ok(version.spec_version)
    })
    .await
}

// A dumb retry function that retries forever.
async fn retry<T, Func, Fut>(rpc_client: RpcClient, url: &str, f: Func) -> T
where
    Func: Fn(RpcClient) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut rpc_client = Some(rpc_client);

    loop {
        // Try to create a client until success.
        let client = match &rpc_client {
            Some(rpc_client) => rpc_client,
            None => {
                match RpcClient::from_insecure_url(url).await {
                    Ok(client) => rpc_client = Some(client),
                    Err(e) => eprintln!("{e:?}"),
                };
                continue;
            }
        };

        // On error, loop and create a new client to try again.
        match f(client.clone()).await {
            Ok(val) => return val,
            Err(e) => {
                eprintln!("{e:?}");
                rpc_client = None;
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SpecVersionUpdate {
    pub block: u64,
    pub spec_version: u32,
}
