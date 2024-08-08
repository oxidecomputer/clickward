// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use derive_more::{Add, AddAssign, Display, From};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::process::{Command, Stdio};

pub mod config;
use config::*;

mod keeper;
pub use keeper::{KeeperClient, KeeperError};

/// We put things in a subdirectory of the user path for easy cleanup
pub const DEPLOYMENT_DIR: &str = "deployment";

/// The name of the file where `ClickwardMetadata` lives. This is *always*
/// directly below <path>/deployment.
pub const CLICKWARD_META_FILENAME: &str = "clickward-metadata.json";

const MISSING_META: &str = "No deployment found: Is your path correct?";

/// A unique ID for a clickhouse keeper
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    From,
    Add,
    AddAssign,
    Display,
    Serialize,
    Deserialize,
)]
pub struct KeeperId(pub u64);

/// A unique ID for a clickhouse server
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    From,
    Add,
    AddAssign,
    Display,
    Serialize,
    Deserialize,
)]
pub struct ServerId(pub u64);

pub const DEFAULT_BASE_PORTS: BasePorts = BasePorts {
    keeper: 20000,
    raft: 21000,
    clickhouse_tcp: 22000,
    clickhouse_http: 23000,
    clickhouse_interserver_http: 24000,
};

// A configuration for a given clickward deployment
pub struct DeploymentConfig {
    pub path: Utf8PathBuf,
    pub base_ports: BasePorts,
    pub cluster_name: String,
}

impl DeploymentConfig {
    pub fn new_with_default_ports<S: Into<String>>(
        path: Utf8PathBuf,
        cluster_name: S,
        target_dir: Option<Utf8PathBuf>,
    ) -> DeploymentConfig {
        let dir = match target_dir {
            Some(d) => d,
            None => Utf8PathBuf::from(DEPLOYMENT_DIR),
        };
        let path = path.join(dir);
        DeploymentConfig {
            path,
            base_ports: DEFAULT_BASE_PORTS,
            cluster_name: cluster_name.into(),
        }
    }
}

// Port allocation used for config generation
pub struct BasePorts {
    pub keeper: u16,
    pub raft: u16,
    pub clickhouse_tcp: u16,
    pub clickhouse_http: u16,
    pub clickhouse_interserver_http: u16,
}

/// Metadata stored for use by clickward
///
/// This prevents the need to parse XML and only includes what we need to
/// implement commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickwardMetadata {
    /// IDs of keepers that are currently part of the cluster
    /// We never reuse IDs.
    pub keeper_ids: BTreeSet<KeeperId>,

    /// The maximum allocated keeper_id so far
    /// We only ever increment when adding a new id.
    pub max_keeper_id: KeeperId,

    /// IDs of clickhouse servers
    /// We never reuse IDs.
    pub server_ids: BTreeSet<ServerId>,

    /// The maximum allocated clickhouse server id so far
    /// We only ever increment when adding a new id.
    pub max_server_id: ServerId,
}

impl ClickwardMetadata {
    pub fn new(
        keeper_ids: BTreeSet<KeeperId>,
        replica_ids: BTreeSet<ServerId>,
    ) -> ClickwardMetadata {
        let max_keeper_id = *keeper_ids.last().unwrap();
        let max_replica_id = *replica_ids.last().unwrap();
        ClickwardMetadata {
            keeper_ids,
            max_keeper_id,
            server_ids: replica_ids,
            max_server_id: max_replica_id,
        }
    }

    pub fn add_keeper(&mut self) -> KeeperId {
        self.max_keeper_id += 1.into();
        self.keeper_ids.insert(self.max_keeper_id);
        self.max_keeper_id
    }

    pub fn remove_keeper(&mut self, id: KeeperId) -> Result<()> {
        let was_removed = self.keeper_ids.remove(&id);
        if !was_removed {
            bail!("No such keeper: {id}");
        }
        Ok(())
    }

    pub fn add_server(&mut self) -> ServerId {
        self.max_server_id += 1.into();
        self.server_ids.insert(self.max_server_id);
        self.max_server_id
    }

    pub fn remove_server(&mut self, id: ServerId) -> Result<()> {
        let was_removed = self.server_ids.remove(&id);
        if !was_removed {
            bail!("No such replica: {id}");
        }
        Ok(())
    }

    pub fn load(deployment_dir: &Utf8Path) -> Result<ClickwardMetadata> {
        let path = deployment_dir.join(CLICKWARD_META_FILENAME);
        let json = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {path}"))?;
        let meta = serde_json::from_str(&json)?;
        Ok(meta)
    }

    pub fn save(&self, deployment_dir: &Utf8Path) -> Result<()> {
        let path = deployment_dir.join(CLICKWARD_META_FILENAME);
        let json = serde_json::to_string(self)?;
        std::fs::write(&path, &json)
            .with_context(|| format!("Failed to write {path}"))?;
        Ok(())
    }
}

/// A deployment of Clickhouse servers and Keeper clusters
///
/// This always generates clusters on localhost and is suitable only for testing
pub struct Deployment {
    config: DeploymentConfig,
    meta: Option<ClickwardMetadata>,
}

impl Deployment {
    pub fn new_with_default_port_config<S: Into<String>>(
        path: Utf8PathBuf,
        cluster_name: S,
        target_dir: Option<Utf8PathBuf>,
    ) -> Deployment {
        let config = DeploymentConfig::new_with_default_ports(
            path,
            cluster_name,
            target_dir,
        );
        Deployment::new(config)
    }

    pub fn new(config: DeploymentConfig) -> Deployment {
        let meta = ClickwardMetadata::load(&config.path).ok();
        Deployment { config, meta }
    }

    pub fn meta(&self) -> &Option<ClickwardMetadata> {
        &self.meta
    }

    /// Return the expected clickhouse http port for a given server id
    pub fn http_port(&self, id: ServerId) -> u16 {
        self.config.base_ports.clickhouse_http + id.0 as u16
    }

    /// Return the expected localhost http addr for a given server id
    pub fn http_addr(&self, id: ServerId) -> Result<SocketAddr> {
        let port = self.http_port(id);
        let addr: SocketAddr = format!("[::1]:{port}")
            .parse()
            .context("failed to create address")?;
        Ok(addr)
    }

    pub fn keeper_port(&self, id: KeeperId) -> u16 {
        self.config.base_ports.keeper + id.0 as u16
    }

    pub fn keeper_addr(&self, id: KeeperId) -> Result<SocketAddr> {
        let port = self.keeper_port(id);
        let addr: SocketAddr = format!("[::1]:{port}")
            .parse()
            .context("failed to create address")?;
        Ok(addr)
    }

    /// Stop all clickhouse servers and keepers
    pub fn teardown(&self) -> Result<()> {
        if let Some(meta) = &self.meta {
            // We don't keep track of which nodes we already stopped, and so we
            // allow stopping to fail.
            for id in &meta.keeper_ids {
                // TODO: Logging?
                let _ = self.stop_keeper(*id);
            }
            for id in &meta.server_ids {
                // TODO: Logging?
                let _ = self.stop_server(*id);
            }
        }
        Ok(())
    }

    /// Add a node to clickhouse keeper config at all replicas and start the new
    /// keeper
    pub fn add_keeper(&mut self) -> Result<()> {
        let path = &self.config.path;
        let (new_id, meta) = if let Some(meta) = &mut self.meta {
            let new_id = meta.add_keeper();
            println!("Updating config to include new keeper: {new_id}");
            meta.save(path)?;
            (new_id, meta.clone())
        } else {
            bail!(MISSING_META);
        };

        // We update the new node and start it before the other nodes. It must be online
        // for reconfiguration to succeed.
        self.generate_keeper_config(new_id, meta.keeper_ids.clone())?;
        self.start_keeper(new_id)?;

        // Generate new configs for all the other keepers
        // They will automatically reload them.
        let mut other_keepers = meta.keeper_ids.clone();
        other_keepers.remove(&new_id);
        for id in other_keepers {
            self.generate_keeper_config(id, meta.keeper_ids.clone())?;
        }

        // Update clickhouse configs so they know about the new keeper node
        self.generate_clickhouse_config(
            meta.keeper_ids.clone(),
            meta.server_ids.clone(),
        )?;

        Ok(())
    }

    /// Add a new clickhouse server replica
    pub fn add_server(&mut self) -> Result<()> {
        let (new_id, meta) = if let Some(meta) = &mut self.meta {
            let new_id = meta.add_server();
            println!("Updating config to include new replica: {new_id}");
            meta.save(&self.config.path)?;
            (new_id, meta.clone())
        } else {
            bail!(MISSING_META);
        };

        // Update clickhouse configs so they know about the new replica
        self.generate_clickhouse_config(meta.keeper_ids, meta.server_ids)?;

        // Start the new replica
        self.start_server(new_id)?;

        Ok(())
    }

    /// Remove a node from clickhouse keeper config at all replicas and stop the
    /// old replica.
    pub fn remove_keeper(&mut self, id: KeeperId) -> Result<()> {
        println!("Updating config to remove keeper: {id}");
        let meta = if let Some(meta) = &mut self.meta {
            meta.remove_keeper(id)?;
            meta.save(&self.config.path)?;
            meta.clone()
        } else {
            bail!(MISSING_META);
        };

        for id in &meta.keeper_ids {
            self.generate_keeper_config(*id, meta.keeper_ids.clone())?;
        }
        self.stop_keeper(id)?;

        // Update clickhouse configs so they know about the removed keeper node
        self.generate_clickhouse_config(
            meta.keeper_ids.clone(),
            meta.server_ids.clone(),
        )?;

        Ok(())
    }

    /// Remove a node from clickhouse server config at all replicas and stop the
    /// old server.
    pub fn remove_server(&mut self, id: ServerId) -> Result<()> {
        println!("Updating config to remove clickhouse server: {id}");
        let meta = if let Some(meta) = &mut self.meta {
            meta.remove_server(id)?;
            meta.save(&self.config.path)?;
            meta.clone()
        } else {
            bail!(MISSING_META);
        };

        // Update clickhouse configs so they know about the removed keeper node
        self.generate_clickhouse_config(meta.keeper_ids, meta.server_ids)?;

        // Stop the clickhouse server
        self.stop_server(id)?;

        Ok(())
    }

    pub fn start_keeper(&self, id: KeeperId) -> Result<()> {
        let dir = self.config.path.join(format!("keeper-{id}"));
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
            .context("Failed to start keeper")?;
        Ok(())
    }

    pub fn start_server(&self, id: ServerId) -> Result<()> {
        let dir = self.config.path.join(format!("clickhouse-{id}"));
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
            .context("Failed to start clickhouse server")?;
        Ok(())
    }

    pub fn stop_keeper(&self, id: KeeperId) -> Result<()> {
        let dir = self.config.path.join(format!("keeper-{id}"));
        let pidfile = dir.join("keeper.pid");
        let pid = std::fs::read_to_string(&pidfile)?;
        let pid = pid.trim_end();
        println!("Stopping keeper: {dir} at pid {pid}");
        Command::new("kill")
            .arg("-9")
            .arg(pid)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("Failed to kill keeper")?;
        std::fs::remove_file(&pidfile)?;
        Ok(())
    }

    pub fn stop_server(&self, id: ServerId) -> Result<()> {
        let name = format!("clickhouse-{id}");
        let dir = self.config.path.join(&name);
        let pidfile = dir.join("clickhouse.pid");
        let pid = std::fs::read_to_string(&pidfile)?;
        let pid = pid.trim_end();

        // Retrieve the child process id
        let output = Command::new("pgrep")
            .arg("-P")
            .arg(pid)
            .output()
            .context("failed to retreive child process for pid {pid}")?;
        let child_pid = String::from_utf8(output.stdout)
            .context("failed to parse child pid for pid {pid}")?;
        let child_pid = child_pid.trim_end();

        println!("Stopping clickhouse server {name}: pid - {pid}, child pid - {child_pid}");

        // Kill the parent
        Command::new("kill")
            .arg("-9")
            .arg(pid)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("Failed to kill clickhouse server")?;

        // Kill the child
        Command::new("kill")
            .arg("-9")
            .arg(child_pid)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("Failed to kill clickhouse server")?;
        std::fs::remove_file(&pidfile)?;

        Ok(())
    }

    /// Deploy our clickhouse replicas and keeper cluster
    pub fn deploy(&self) -> Result<()> {
        let dirs: Vec<_> = self.config.path.read_dir_utf8()?.collect();

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
                .context("Failed to start keeper")?;
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
                .context("Failed to start clickhouse server")?;
        }

        Ok(())
    }

    /// Generate configuration for our clusters
    pub fn generate_config(
        &mut self,
        num_keepers: u64,
        num_replicas: u64,
    ) -> Result<()> {
        std::fs::create_dir_all(&self.config.path).unwrap();

        let keeper_ids: BTreeSet<KeeperId> =
            (1..=num_keepers).map(KeeperId).collect();
        let replica_ids: BTreeSet<ServerId> =
            (1..=num_replicas).map(ServerId).collect();

        self.generate_clickhouse_config(
            keeper_ids.clone(),
            replica_ids.clone(),
        )?;
        for id in &keeper_ids {
            self.generate_keeper_config(*id, keeper_ids.clone())?;
        }

        let meta = ClickwardMetadata::new(keeper_ids, replica_ids);
        meta.save(&self.config.path)?;
        self.meta = Some(meta);

        Ok(())
    }
    fn generate_clickhouse_config(
        &self,
        keeper_ids: BTreeSet<KeeperId>,
        replica_ids: BTreeSet<ServerId>,
    ) -> Result<()> {
        let cluster = self.config.cluster_name.clone();

        let servers: Vec<_> = replica_ids
            .iter()
            .map(|&id| ServerConfig {
                host: "::1".to_string(),
                port: self.config.base_ports.clickhouse_tcp + id.0 as u16,
            })
            .collect();
        let remote_servers = RemoteServers {
            cluster: cluster.clone(),
            secret: "some-unique-value".to_string(),
            replicas: servers,
        };

        let keepers = KeeperConfigsForReplica {
            nodes: keeper_ids
                .iter()
                .map(|&id| ServerConfig {
                    host: "[::1]".to_string(),
                    port: self.config.base_ports.keeper + id.0 as u16,
                })
                .collect(),
        };

        for id in replica_ids {
            let dir: Utf8PathBuf =
                [self.config.path.as_str(), &format!("clickhouse-{id}")]
                    .iter()
                    .collect();
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
                    replica: id,
                    cluster: cluster.clone(),
                },
                listen_host: "::1".to_string(),
                http_port: self.config.base_ports.clickhouse_http + id.0 as u16,
                tcp_port: self.config.base_ports.clickhouse_tcp + id.0 as u16,
                interserver_http_port: self
                    .config
                    .base_ports
                    .clickhouse_interserver_http
                    + id.0 as u16,
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

    /// Generate a config for `this_keeper` consisting of the replicas in `keeper_ids`
    fn generate_keeper_config(
        &self,
        this_keeper: KeeperId,
        keeper_ids: BTreeSet<KeeperId>,
    ) -> Result<()> {
        let raft_servers: Vec<_> = keeper_ids
            .iter()
            .map(|id| RaftServerConfig {
                id: *id,
                hostname: "::1".to_string(),
                port: self.config.base_ports.raft + id.0 as u16,
            })
            .collect();
        let dir: Utf8PathBuf =
            [self.config.path.as_str(), &format!("keeper-{this_keeper}")]
                .iter()
                .collect();
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
            tcp_port: self.config.base_ports.keeper + this_keeper.0 as u16,
            server_id: this_keeper,
            log_storage_path: dir.join("coordination").join("log"),
            snapshot_storage_path: dir.join("coordination").join("snapshots"),
            coordination_settings: KeeperCoordinationSettings {
                operation_timeout_ms: 10000,
                session_timeout_ms: 30000,
                raft_logs_level: LogLevel::Trace,
            },
            raft_config: RaftServers { servers: raft_servers.clone() },
        };
        let mut f = File::create(dir.join("keeper-config.xml"))?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;

        Ok(())
    }
}
