use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use std::fmt::Display;
use std::fs::File;
use std::io::Write;
use std::net::Ipv6Addr;

pub enum LogLevel {
    Trace,
    Debug,
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
        };
        write!(f, "{s}")
    }
}

/// Config for an individual Clickhouse Replica
pub struct ReplicaConfig {
    pub logger: LogConfig,
    pub macros: Macros,
    pub listen_host: Ipv6Addr,
    pub http_port: u16,
    pub tcp_port: u16,
    pub keepers: KeeperConfigsForReplica,
}

impl ReplicaConfig {
    pub fn to_xml(&self) -> String {
        let ReplicaConfig {
            logger,
            macros,
            listen_host,
            http_port,
            tcp_port,
            keepers,
        } = self;
        let logger = logger.to_xml();
        let cluster = macros.cluster.clone();
        let id = macros.replica;
        let macros = macros.to_xml();
        let keepers = keepers.to_xml();
        format!(
            "{logger}
                 <display_name>{cluster}-{id}</display_name>
                 <listen_host>{listen_host}</listen_host>
                 <http_port>{http_port}</http_port>
                 <tcp_port>{tcp_port}</tcp_port>
             {macros}
             {keepers}
            "
        )
    }
}

pub struct Macros {
    pub shard: u64,
    pub replica: u64,
    pub cluster: String,
}

impl Macros {
    pub fn to_xml(&self) -> String {
        let Macros {
            shard,
            replica,
            cluster,
        } = self;
        format!(
            "
            <macros>
                <shard>{shard}</shard>
                <replica>{replica}</replica>
                <cluster>{cluster}</cluster>
            </macros>
        "
        )
    }
}

pub struct RemoteServers {
    pub cluster: String,
    pub secret: String,
    pub replicas: Vec<ServerConfig>,
}

impl RemoteServers {
    pub fn to_xml(&self) -> String {
        let RemoteServers {
            cluster,
            secret,
            replicas,
        } = self;

        let mut s = format!(
            "
            <remote_servers replace=\"true\">
                <{cluster}>
                    <secret>{secret}</secret>
                    <shard>
                        <internal_replication>true</internal_replication>
        "
        );

        for r in replicas {
            let ServerConfig { host, port } = r;
            s.push_str("                <replica>\n");
            s.push_str(&format!("                    <host>{host}</host>\n"));
            s.push_str(&format!("                    <port>{port}</port>\n"));
            s.push_str("                </replica>\n");
        }

        s.push_str(&format!(
            "
                    </shard>
                </{cluster}>
            </remote_servers>
        "
        ));

        s
    }
}

pub struct KeeperConfigsForReplica {
    pub nodes: Vec<ServerConfig>,
}

impl KeeperConfigsForReplica {
    pub fn to_xml(&self) -> String {
        let mut s = String::from("    <zookeeper>\n");
        for node in &self.nodes {
            let ServerConfig { host, port } = node;
            s.push_str("        <node>\n");
            s.push_str(&format!("            <host>{host}</host>\n"));
            s.push_str(&format!("            <port>{port}</port>\n"));
            s.push_str("        </node>\n");
        }
        s.push_str("    </zookeeper>");
        s
    }
}

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

pub struct LogConfig {
    pub level: LogLevel,
    pub log: Utf8PathBuf,
    pub errorlog: Utf8PathBuf,
    // TODO: stronger type?
    pub size: String,
    pub count: usize,
}

impl LogConfig {
    pub fn to_xml(&self) -> String {
        let LogConfig {
            level,
            log,
            errorlog,
            size,
            count,
        } = &self;
        format!(
            "
    <logger>
        <level>{level}</level>
        <log>{log}</log>
        <errorlog>{errorlog}</errorlog>
        <size>{size}</size>
        <count>{count}</count>
    </logger>
"
        )
    }
}

pub struct KeeperCoordinationSettings {
    pub operation_timeout_ms: u32,
    pub session_timeout_ms: u32,
    pub raft_logs_level: LogLevel,
}

pub struct RaftServers {
    pub servers: Vec<RaftServerConfig>,
}

impl RaftServers {
    pub fn to_xml(&self) -> String {
        let mut s = String::new();
        for server in &self.servers {
            let RaftServerConfig { id, hostname, port } = server;
            s.push_str(&format!(
                "
            <server>
                <id>{id}</id>
                <hostname>{hostname}</hostname>
                <port>{port}</port>
            </server>
            "
            ));
        }

        s
    }
}

#[derive(Debug, Clone)]
pub struct RaftServerConfig {
    pub id: u64,
    pub hostname: String,
    pub port: u16,
}

/// Config for an individual Clickhouse Keeper
pub struct KeeperConfig {
    pub logger: LogConfig,
    pub listen_host: Ipv6Addr,
    pub tcp_port: u16,
    pub server_id: u64,
    pub log_storage_path: Utf8PathBuf,
    pub snapshot_storage_path: Utf8PathBuf,
    pub coordination_settings: KeeperCoordinationSettings,
    pub raft_config: RaftServers,
}

impl KeeperConfig {
    pub fn to_xml(&self) -> String {
        let KeeperConfig {
            logger,
            listen_host,
            tcp_port,
            server_id,
            log_storage_path,
            snapshot_storage_path,
            coordination_settings,
            raft_config,
        } = self;
        let logger = logger.to_xml();
        let KeeperCoordinationSettings {
            operation_timeout_ms,
            session_timeout_ms,
            raft_logs_level,
        } = coordination_settings;
        let raft_servers = raft_config.to_xml();
        format!(
            "
<clickhouse>
{logger}
    <listen_host>{listen_host}</listen_host>
    <keeper_server>
        <tcp_port>{tcp_port}</tcp_port>
        <server_id>{server_id}</server_id>
        <log_storage_path>{log_storage_path}</log_storage_path>
        <snapshot_storage_path>{snapshot_storage_path}</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>{operation_timeout_ms}</operation_timeout_ms>
            <session_timeout_ms>{session_timeout_ms}</session_timeout_ms>
            <raft_logs_level>{raft_logs_level}</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
{raft_servers}
        </raft_configuration>
    </keeper_server>
</clickhouse>
"
        )
    }
}

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
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::GenConfig {
            path,
            num_keepers,
            num_replicas,
        } => {
            generate_config(path, num_keepers, num_replicas);
        }
    };
}

/// Generate configuration for our clusters
fn generate_config(path: Utf8PathBuf, num_keepers: u64, num_replicas: u64) {
    std::fs::create_dir_all(&path).unwrap();
    let keeper_base_port: u16 = 20000;
    let raft_base_port: u16 = 21000;
    let raft_servers: Vec<_> = (1..=num_keepers)
        .map(|id| RaftServerConfig {
            id,
            hostname: format!("clickhouse-keeper-{id}"),
            port: raft_base_port + id as u16,
        })
        .collect();
    for i in 1..=num_keepers {
        let dir: Utf8PathBuf = [path.as_str(), &format!("keeper-{i}")].iter().collect();
        let logs: Utf8PathBuf = dir.clone().join("logs");
        std::fs::create_dir_all(&logs).unwrap();
        let log = logs.clone().join("clickhouse-keeper.log");
        let errorlog = logs.clone().join("clickhouse-keeper.err.log");
        let config = KeeperConfig {
            logger: LogConfig {
                level: LogLevel::Debug,
                log,
                errorlog,
                size: "100M".to_string(),
                count: 1,
            },
            listen_host: "::1".parse().unwrap(),
            tcp_port: keeper_base_port + i as u16,
            server_id: 1,
            log_storage_path: dir.clone().join("coordination").join("log"),
            snapshot_storage_path: dir.clone().join("coordination").join("snapshots"),
            coordination_settings: KeeperCoordinationSettings {
                operation_timeout_ms: 10000,
                session_timeout_ms: 30000,
                raft_logs_level: LogLevel::Trace,
            },
            raft_config: RaftServers {
                servers: raft_servers.clone(),
            },
        };
        let mut f = File::create(dir.clone().join("keeper-config.xml")).unwrap();
        f.write_all(config.to_xml().as_bytes()).unwrap();
        f.flush().unwrap();
    }
}
