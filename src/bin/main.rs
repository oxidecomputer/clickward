// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};

use clickward::{Deployment, KeeperClient};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Generate configuration for our clickhouse and keeper clusters
    GenConfig {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Number of clickhouse keepers
        #[arg(long)]
        num_keepers: u64,

        /// Number of clickhouse replicas
        #[arg(long)]
        num_replicas: u64,

        /// Target directory where all configuration files will be saved
        #[arg(short, long)]
        target_dir: Option<Utf8PathBuf>,
    },

    /// Launch our deployment given generated configs
    Deploy {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Target directory where all configuration files will be saved
        #[arg(short, long)]
        target_dir: Option<Utf8PathBuf>,
    },

    /// Stop all our deployed processes
    Teardown {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Target directory where all configuration files will be saved
        #[arg(short, long)]
        target_dir: Option<Utf8PathBuf>,
    },

    /// Show metadata about the deployment
    Show {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Target directory where all configuration files will be saved
        #[arg(short, long)]
        target_dir: Option<Utf8PathBuf>,
    },

    /// Add a keeper node to the keeper cluster
    AddKeeper {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Target directory where all configuration files will be saved
        #[arg(short, long)]
        target_dir: Option<Utf8PathBuf>,
    },

    /// Remove a keeper node
    RemoveKeeper {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Id of the keeper node to remove
        #[arg(long)]
        id: u64,

        /// Target directory where all configuration files will be saved
        #[arg(short, long)]
        target_dir: Option<Utf8PathBuf>,
    },

    /// Get the keeper config from a given keeper
    KeeperConfig {
        /// Id of the keeper node to remove
        #[arg(long)]
        id: u64,
    },

    /// Add a clickhouse server
    AddServer {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Target directory where all configuration files will be saved
        #[arg(short, long)]
        target_dir: Option<Utf8PathBuf>,
    },

    /// Remove a clickhouse server
    RemoveServer {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Id of the clickhouse server node to remove
        #[arg(long)]
        id: u64,

        /// Target directory where all configuration files will be saved
        #[arg(short, long)]
        target_dir: Option<Utf8PathBuf>,
    },
}

//const CLUSTER: &str = "test_cluster";
const CLUSTER: &str = "oximeter_cluster";

#[tokio::main]
async fn main() {
    if let Err(e) = handle().await {
        println!("Error: {e}");
    }
}

async fn handle() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::GenConfig { path, num_keepers, num_replicas, target_dir } => {
            let mut d = Deployment::new_with_default_port_config(
                path, CLUSTER, target_dir,
            );
            d.generate_config(num_keepers, num_replicas)
        }
        Commands::Deploy { path, target_dir } => {
            let d = Deployment::new_with_default_port_config(
                path, CLUSTER, target_dir,
            );
            d.deploy()
        }
        Commands::Teardown { path, target_dir } => {
            let d = Deployment::new_with_default_port_config(
                path, CLUSTER, target_dir,
            );
            d.teardown()
        }
        Commands::Show { path, target_dir } => {
            let d = Deployment::new_with_default_port_config(
                path, CLUSTER, target_dir,
            );
            match &d.meta() {
                Some(meta) => println!("{:#?}", meta),
                None => println!(
                    "No deployment generated: Please call `gen-config`"
                ),
            }
            Ok(())
        }
        Commands::AddKeeper { path, target_dir } => {
            let mut d = Deployment::new_with_default_port_config(
                path, CLUSTER, target_dir,
            );
            d.add_keeper()
        }
        Commands::RemoveKeeper { path, id, target_dir } => {
            let mut d = Deployment::new_with_default_port_config(
                path, CLUSTER, target_dir,
            );
            d.remove_keeper(id.into())
        }
        Commands::KeeperConfig { id } => {
            // Unused
            let dummy_path = ".".into();
            let d = Deployment::new_with_default_port_config(
                dummy_path, CLUSTER, None,
            );
            let addr = d.keeper_addr(id.into())?;
            let zk = KeeperClient::new(addr);
            let output = zk.config().await?;
            println!("{output:#?}");
            Ok(())
        }
        Commands::AddServer { path, target_dir } => {
            let mut d = Deployment::new_with_default_port_config(
                path, CLUSTER, target_dir,
            );
            d.add_server()
        }
        Commands::RemoveServer { path, id, target_dir } => {
            let mut d = Deployment::new_with_default_port_config(
                path, CLUSTER, target_dir,
            );
            d.remove_server(id.into())
        }
    }
}
