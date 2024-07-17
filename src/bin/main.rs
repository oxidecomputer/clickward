// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};

use clickward::Deployment;

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
    },

    /// Launch our deployment given generated configs
    Deploy {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,
    },

    /// Stop all our deployed processes
    Teardown {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,
    },

    /// Show metadata about the deployment
    Show {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,
    },

    /// Add a keeper node to the keeper cluster
    AddKeeper {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,
    },

    /// Remove a keeper node
    RemoveKeeper {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Id of the keeper node to remove
        #[arg(long)]
        id: u64,
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
    },

    /// Remove a clickhouse server
    RemoveServer {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,

        /// Id of the clickhouse server node to remove
        #[arg(long)]
        id: u64,
    },
}

const CLUSTER: &str = "test_cluster";

fn main() {
    let cli = Cli::parse();
    let res = match cli.command {
        Commands::GenConfig {
            path,
            num_keepers,
            num_replicas,
        } => {
            let d = Deployment::new_with_default_port_config(path, CLUSTER);
            d.generate_config(num_keepers, num_replicas)
        }
        Commands::Deploy { path } => {
            let d = Deployment::new_with_default_port_config(path, CLUSTER);
            d.deploy()
        }
        Commands::Teardown { path } => {
            let d = Deployment::new_with_default_port_config(path, CLUSTER);
            d.teardown()
        }
        Commands::Show { path } => {
            let d = Deployment::new_with_default_port_config(path, CLUSTER);
            d.show()
        }
        Commands::AddKeeper { path } => {
            let d = Deployment::new_with_default_port_config(path, CLUSTER);
            d.add_keeper()
        }
        Commands::RemoveKeeper { path, id } => {
            let d = Deployment::new_with_default_port_config(path, CLUSTER);
            d.remove_keeper(id)
        }
        Commands::KeeperConfig { id } => {
            // Unused
            let dummy_path = ".".into();
            let d = Deployment::new_with_default_port_config(dummy_path, CLUSTER);
            d.keeper_config(id)
        }
        Commands::AddServer { path } => {
            let d = Deployment::new_with_default_port_config(path, CLUSTER);
            d.add_server()
        }
        Commands::RemoveServer { path, id } => {
            let d = Deployment::new_with_default_port_config(path, CLUSTER);
            d.remove_server(id)
        }
    };

    if let Err(e) = res {
        println!("Error: {e}");
    }
}
