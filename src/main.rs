use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};

mod config;
use config::*;

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
    RemoveKeeperNode {
        /// Root path of all configuration
        #[arg(short, long)]
        path: Utf8PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();
    let res = match cli.command {
        Commands::GenConfig {
            path,
            num_keepers,
            num_replicas,
        } => generate_config(path, num_keepers, num_replicas),
        Commands::Deploy { path } => deploy(path),
        Commands::Show { path } => show(path),
        Commands::AddKeeper { path } => add_keeper(path),
        Commands::RemoveKeeperNode { path } => Ok(()),
    };

    if let Err(e) = res {
        println!("Error: {e}");
    }
}

const KEEPER_BASE_PORT: u16 = 20000;
const RAFT_BASE_PORT: u16 = 21000;
const CLICKHOUSE_BASE_TCP_PORT: u16 = 22000;
const CLICKHOUSE_BASE_HTTP_PORT: u16 = 23000;
const CLICKHOUSE_BASE_INTERSERVER_HTTP_PORT: u16 = 24000;

/// We put things in a subdirectory of the user path for easy cleanup
const DEPLOYMENT_DIR: &str = "deployment";

/// The name of the file where `ClickwardMetadata` lives. This is *always*
/// directly below <path>/deployment.
const CLICKWARD_META_FILENAME: &str = "clickward-metadata.json";

/// Metadata stored for use by clickward
///
/// This prevents the need to parse XML and only includes what we need to
/// implement commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickwardMetadata {
    /// IDs of keepers that are currently part of the cluster
    /// We never reuse IDs.
    pub keeper_ids: BTreeSet<u64>,

    /// The maximum allocated keeper_id so far
    /// We only ever increment when adding a new id.
    pub max_keeper_id: u64,
}

impl ClickwardMetadata {
    pub fn new(keeper_ids: BTreeSet<u64>) -> ClickwardMetadata {
        ClickwardMetadata {
            keeper_ids,
            max_keeper_id: 3,
        }
    }

    pub fn add_keeper(&mut self) -> u64 {
        self.max_keeper_id += 1;
        self.keeper_ids.insert(self.max_keeper_id);
        self.max_keeper_id
    }

    pub fn load(deployment_dir: &Utf8Path) -> Result<ClickwardMetadata> {
        let path = deployment_dir.join(CLICKWARD_META_FILENAME);
        let json =
            std::fs::read_to_string(&path).with_context(|| format!("failed to read {path}"))?;
        let meta = serde_json::from_str(&json)?;
        Ok(meta)
    }

    pub fn save(&self, deployment_dir: &Utf8Path) -> Result<()> {
        let path = deployment_dir.join(CLICKWARD_META_FILENAME);
        let json = serde_json::to_string(self)?;
        std::fs::write(&path, &json).with_context(|| format!("Failed to write {path}"))?;
        Ok(())
    }
}

fn show(path: Utf8PathBuf) -> Result<()> {
    let path = path.join(DEPLOYMENT_DIR);
    let meta = ClickwardMetadata::load(&path)?;
    println!("{:#?}", meta);
    Ok(())
}

/// Add a node to clickhouse keeper config at all replicas and start the new
/// keeper
fn add_keeper(path: Utf8PathBuf) -> Result<()> {
    let path = path.join(DEPLOYMENT_DIR);
    let mut meta = ClickwardMetadata::load(&path)?;
    let new_id = meta.add_keeper();

    println!("Updating config to include new keeper: {new_id}");

    // The writes from the following two functions aren't transactional
    // Don't worry about it.
    meta.save(&path)?;
    generate_keeper_config(&path, meta.keeper_ids.clone())?;
    start_keeper(&path, new_id);

    Ok(())
}

fn start_keeper(path: &Utf8Path, id: u64) {
    let dir = path.join(format!("keeper-{id}"));
    println!("Deploying keeper: {dir}");
    let config = dir.join("keeper-config.xml");
    let pidfile = dir.join("keeper.pid");
    Command::new("clickhouse")
        .arg("keeper")
        .arg("-C")
        .arg(config)
        .arg("--pidfile")
        .arg(pidfile)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start keeper");
}

/// Deploy our clickhouse replicas and keeper cluster
fn deploy(path: Utf8PathBuf) -> Result<()> {
    let path = path.join(DEPLOYMENT_DIR);
    let dirs: Vec<_> = path.read_dir_utf8()?.collect();

    // Find all keeper replicas them
    let keeper_dirs = dirs.iter().filter_map(|e| {
        let entry = e.as_ref().unwrap();
        if entry.path().file_name().unwrap().starts_with("keeper") {
            Some(entry.path())
        } else {
            None
        }
    });
    // Start all keepers
    for dir in keeper_dirs {
        println!("Deploying keeper: {dir}");
        let config = dir.join("keeper-config.xml");
        let pidfile = dir.join("keeper.pid");
        Command::new("clickhouse")
            .arg("keeper")
            .arg("-C")
            .arg(config)
            .arg("--pidfile")
            .arg(pidfile)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start keeper");
    }

    // Find all clickhouse replicas
    let clickhouse_dirs = dirs.iter().filter_map(|e| {
        let entry = e.as_ref().unwrap();
        if entry.path().file_name().unwrap().starts_with("clickhouse") {
            Some(entry.path())
        } else {
            None
        }
    });

    // Start all clickhouse servers
    for dir in clickhouse_dirs {
        println!("Deploying clickhouse server: {dir}");
        let config = dir.join("clickhouse-config.xml");
        let pidfile = dir.join("clickhouse.pid");
        Command::new("clickhouse")
            .arg("server")
            .arg("-C")
            .arg(config)
            .arg("--pidfile")
            .arg(pidfile)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start clickhouse server");
    }
    Ok(())
}

/// Generate configuration for our clusters
fn generate_config(path: Utf8PathBuf, num_keepers: u64, num_replicas: u64) -> Result<()> {
    let path = path.join(DEPLOYMENT_DIR);
    std::fs::create_dir_all(&path).unwrap();
    generate_clickhouse_config(&path, num_keepers, num_replicas)?;

    let keeper_ids: BTreeSet<u64> = (1..=num_keepers).collect();
    generate_keeper_config(&path, keeper_ids.clone())?;

    let meta = ClickwardMetadata::new(keeper_ids);
    meta.save(&path)?;

    Ok(())
}

fn generate_clickhouse_config(path: &Utf8Path, num_keepers: u64, num_replicas: u64) -> Result<()> {
    let cluster = "test_cluster".to_string();

    let servers: Vec<_> = (1..=num_replicas)
        .map(|id| ServerConfig {
            host: "::1".to_string(),
            port: CLICKHOUSE_BASE_TCP_PORT + id as u16,
        })
        .collect();
    let remote_servers = RemoteServers {
        cluster: cluster.clone(),
        secret: "some-unique-value".to_string(),
        replicas: servers,
    };

    let keepers = KeeperConfigsForReplica {
        nodes: (1..=num_keepers)
            .map(|id| ServerConfig {
                host: "[::1]".to_string(),
                port: KEEPER_BASE_PORT + id as u16,
            })
            .collect(),
    };

    for i in 1..=num_replicas {
        let dir: Utf8PathBuf = [path.as_str(), &format!("clickhouse-{i}")].iter().collect();
        let logs: Utf8PathBuf = dir.join("logs");
        std::fs::create_dir_all(&logs)?;
        let log = logs.join("clickhouse.log");
        let errorlog = logs.join("clickhouse.err.log");
        let data_path = dir.join("data");
        let config = ReplicaConfig {
            logger: LogConfig {
                level: LogLevel::Trace,
                log,
                errorlog,
                size: "100M".to_string(),
                count: 1,
            },
            macros: Macros {
                shard: 1,
                replica: i,
                cluster: cluster.clone(),
            },
            listen_host: "::1".to_string(),
            http_port: CLICKHOUSE_BASE_HTTP_PORT + i as u16,
            tcp_port: CLICKHOUSE_BASE_TCP_PORT + i as u16,
            interserver_http_port: CLICKHOUSE_BASE_INTERSERVER_HTTP_PORT + i as u16,
            remote_servers: remote_servers.clone(),
            keepers: keepers.clone(),
            data_path,
        };
        let mut f = File::create(dir.join("clickhouse-config.xml"))?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;
    }
    Ok(())
}

fn generate_keeper_config(path: &Utf8Path, keeper_ids: BTreeSet<u64>) -> Result<()> {
    let raft_servers: Vec<_> = keeper_ids
        .iter()
        .map(|id| RaftServerConfig {
            id: *id,
            hostname: "::1".to_string(),
            port: RAFT_BASE_PORT + *id as u16,
        })
        .collect();
    for id in keeper_ids.iter() {
        let dir: Utf8PathBuf = [path.as_str(), &format!("keeper-{id}")].iter().collect();
        let logs: Utf8PathBuf = dir.join("logs");
        std::fs::create_dir_all(&logs)?;
        let log = logs.join("clickhouse-keeper.log");
        let errorlog = logs.join("clickhouse-keeper.err.log");
        let config = KeeperConfig {
            logger: LogConfig {
                level: LogLevel::Trace,
                log,
                errorlog,
                size: "100M".to_string(),
                count: 1,
            },
            listen_host: "::1".to_string(),
            tcp_port: KEEPER_BASE_PORT + *id as u16,
            server_id: *id,
            log_storage_path: dir.join("coordination").join("log"),
            snapshot_storage_path: dir.join("coordination").join("snapshots"),
            coordination_settings: KeeperCoordinationSettings {
                operation_timeout_ms: 10000,
                session_timeout_ms: 30000,
                raft_logs_level: LogLevel::Trace,
            },
            raft_config: RaftServers {
                servers: raft_servers.clone(),
            },
        };
        let mut f = File::create(dir.join("keeper-config.xml"))?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;
    }
    Ok(())
}
