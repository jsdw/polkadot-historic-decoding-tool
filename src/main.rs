mod commands;
mod decoding;
mod utils;

use clap::Parser;

#[derive(Parser)]
enum Commands {
    /// Decode blocks, printing the decoded output.
    DecodeBlocks(commands::decode_blocks::Opts),
    /// Decode storage items, printing the decoded output.
    DecodeStorageItems(commands::decode_storage_items::Opts),
    /// Fetch the metadata at a given block as JSON.
    ///
    /// Note: uses state_getMetadata for historic support and will not hand back anything
    /// greater than V14 metadata as a result.
    FetchMetadata(commands::fetch_metadata::Opts),
    /// Fetch all of the metadatas for some chain given a spec version update list.
    /// This will obtain the spec versions internally if none are given.
    ///
    /// Note: uses state_getMetadata for historic support and will not hand back anything
    /// greater than V14 metadata as a result.
    FetchMetadatas(commands::fetch_metadatas::Opts),
    /// Find the block numbers where spec version changes happen.
    /// This is where the metadata/node API may have changed.
    FindSpecChanges(commands::find_spec_changes::Opts),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cmd = Commands::parse();

    match cmd {
        Commands::DecodeBlocks(opts) => {
            commands::decode_blocks::run(opts).await?;
        }
        Commands::DecodeStorageItems(opts) => {
            commands::decode_storage_items::run(opts).await?;
        }
        Commands::FetchMetadata(opts) => {
            commands::fetch_metadata::run(opts).await?;
        }
        Commands::FetchMetadatas(opts) => {
            commands::fetch_metadatas::run(opts).await?;
        }
        Commands::FindSpecChanges(opts) => {
            commands::find_spec_changes::run(opts).await?;
        }
    }

    Ok(())
}
