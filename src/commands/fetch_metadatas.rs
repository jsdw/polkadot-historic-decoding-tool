use crate::commands::find_spec_changes;
use anyhow::Context;
use clap::Parser;
use parity_scale_codec::Encode;
use super::find_spec_changes::{get_spec_version_changes, SpecVersionUpdate};
use std::path::PathBuf;
use std::io::Write;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Opts {
    /// URL of the node to connect to.
    /// Defaults to using Polkadot RPC URLs if not given.
    #[arg(short, long)]
    url: Option<String>,

    /// Spec version updates.
    #[arg(short, long)]
    spec_versions: Option<PathBuf>,

    /// Only save metadatas with unique versions (so we end up with 1 metadata V9, 1 metadata V10, 1 metadata V11 and so on).
    #[arg(long)]
    only_unique_versions: bool,

    /// As binary?
    #[arg(long)]
    binary: bool,

    /// Path to save to.
    #[arg(short, long)]
    output: std::path::PathBuf,
}

pub async fn run(opts: Opts) -> anyhow::Result<()> {
    let spec_versions = match opts.spec_versions {
        Some(path) => {
            // Load existing spec versions from path
            let spec_versions_str =
                std::fs::read_to_string(path).with_context(|| "Could not load spec versions")?;
            serde_json::from_str::<Vec<SpecVersionUpdate>>(&spec_versions_str)
                .with_context(|| "Could not parse spec version JSON")?
        }
        None => {
            // discover spec versions (this is slow)
            get_spec_version_changes(find_spec_changes::Opts {
                url: opts.url.clone(),
                starting_block: None,
                ending_block: None,
            })
            .await?
        }
    };

    let mut last_seen_version = 0;
    let is_binary = opts.binary;
    let path_is_dir = opts.output.is_dir();
    for spec_version in spec_versions {
        let spec = spec_version.spec_version;

        eprintln!("Fetching metadata for spec version {spec}");
        let metadata = super::fetch_metadata::fetch_metadata(
            opts.url.clone(), 
            spec_version.block
        ).await?;

        // eg v14,v15,v16
        let version = metadata.version();

        // Skip this metadata if the version has been seen before.
        if version == last_seen_version && opts.only_unique_versions {
            continue
        }

        last_seen_version = version;

        let save_path = {
            let mut path = opts.output.clone();
            let ext = if is_binary { "scale" } else { "json" };

            if path_is_dir {
                path.push(format!("metadata_v{version}_{spec}.{ext}"));
                path
            } else {
                let file_name = path
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or("metadata");
                let new_file_name = format!("{file_name}_v{version}_{spec}.{ext}");
                path.set_file_name(new_file_name);
                path
            }
        };

        let mut file = std::fs::File::create(save_path)?;

        if opts.binary {
            let encoded = metadata.encode();
            file.write_all(&encoded)?;
        } else {
            serde_json::to_writer_pretty(file, &metadata)?;
        }
    }

    Ok(())
}
