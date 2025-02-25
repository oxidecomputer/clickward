// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{KeeperId, ServerId};
use camino::Utf8PathBuf;
use schemars::{
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
    JsonSchema,
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}

/// Config for an individual Clickhouse Replica
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct ReplicaConfig {
    pub logger: LogConfig,
    pub macros: Macros,
    pub listen_host: String,
    pub http_port: u16,
    pub tcp_port: u16,
    pub interserver_http_port: u16,
    pub remote_servers: RemoteServers,
    pub keepers: KeeperConfigsForReplica,
    #[schemars(schema_with = "path_schema")]
    pub data_path: Utf8PathBuf,
}

impl ReplicaConfig {
    pub fn to_xml(&self) -> String {
        let ReplicaConfig {
            logger,
            macros,
            listen_host,
            http_port,
            tcp_port,
            interserver_http_port,
            remote_servers,
            keepers,
            data_path,
        } = self;
        let logger = logger.to_xml();
        let cluster = macros.cluster.clone();
        let id = macros.replica;
        let macros = macros.to_xml();
        let keepers = keepers.to_xml();
        let remote_servers = remote_servers.to_xml();
        let user_files_path = data_path.clone().join("user_files");
        //let access_path = data_path.clone().join("access");
        let format_schema_path = data_path.clone().join("format_schemas");
        format!(
            "
<clickhouse>
{logger}
    <path>{data_path}</path>

    <profiles>
        <default>
            <opentelemetry_start_trace_probability>1</opentelemetry_start_trace_probability>
            <load_balancing>random</load_balancing>
        </default>

    </profiles>

    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>

    <user_files_path>{user_files_path}</user_files_path>
    <default_profile>default</default_profile>
    <format_schema_path>{format_schema_path}</format_schema_path>
    <display_name>{cluster}-{id}</display_name>
    <listen_host>{listen_host}</listen_host>
    <http_port>{http_port}</http_port>
    <tcp_port>{tcp_port}</tcp_port>
    <interserver_http_port>{interserver_http_port}</interserver_http_port>
    <interserver_http_host>::1</interserver_http_host>
    <distributed_ddl>
        <!-- Cleanup settings (active tasks will not be removed) -->

        <!-- Controls task TTL (default 1 week) -->
        <task_max_lifetime>604800</task_max_lifetime>

        <!-- Controls how often cleanup should be performed (in seconds) -->
        <cleanup_delay_period>60</cleanup_delay_period>

        <!-- Controls how many tasks could be in the queue -->
        <max_tasks_in_queue>1000</max_tasks_in_queue>
     </distributed_ddl>
{macros}
{remote_servers}
{keepers}

    <!-- 
        In newer versions of ClickHouse this table is created automatically.
        We should remove this block once we update to a newer version of 
        ClickHouse that does not need the system.opentelemetry_span_log
        table to be created via the config.xml file
    -->
    <opentelemetry_span_log>
        <engine>
            engine MergeTree
            partition by toYYYYMM(finish_date)
            order by (finish_date, finish_time_us, trace_id)
        </engine>
        <database>system</database>
        <table>opentelemetry_span_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </opentelemetry_span_log>

    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>

    <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>

</clickhouse>
"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct Macros {
    pub shard: u64,
    pub replica: ServerId,
    pub cluster: String,
}

impl Macros {
    pub fn to_xml(&self) -> String {
        let Macros { shard, replica, cluster } = self;
        format!(
            "
    <macros>
        <shard>{shard}</shard>
        <replica>{replica}</replica>
        <cluster>{cluster}</cluster>
    </macros>"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct RemoteServers {
    pub cluster: String,
    pub secret: String,
    pub replicas: Vec<ServerConfig>,
}

impl RemoteServers {
    pub fn to_xml(&self) -> String {
        let RemoteServers { cluster, secret, replicas } = self;

        let mut s = format!(
            "
    <remote_servers replace=\"true\">
        <{cluster}>
            <secret>{secret}</secret>
            <shard>
                <internal_replication>true</internal_replication>"
        );

        for r in replicas {
            let ServerConfig { host, port } = r;
            s.push_str(&format!(
                "
                <replica>
                    <host>{host}</host>
                    <port>{port}</port>
                </replica>"
            ));
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

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct KeeperConfigsForReplica {
    pub nodes: Vec<ServerConfig>,
}

impl KeeperConfigsForReplica {
    pub fn to_xml(&self) -> String {
        let mut s = String::from("    <zookeeper>");
        for node in &self.nodes {
            let ServerConfig { host, port } = node;
            s.push_str(&format!(
                "
        <node>
            <host>{host}</host>
            <port>{port}</port>
        </node>",
            ));
        }
        s.push_str("\n    </zookeeper>");
        s
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct LogConfig {
    pub level: LogLevel,
    #[schemars(schema_with = "path_schema")]
    pub log: Utf8PathBuf,
    #[schemars(schema_with = "path_schema")]
    pub errorlog: Utf8PathBuf,
    // TODO: stronger type?
    pub size: String,
    pub count: usize,
}

impl LogConfig {
    pub fn to_xml(&self) -> String {
        let LogConfig { level, log, errorlog, size, count } = &self;
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

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct KeeperCoordinationSettings {
    pub operation_timeout_ms: u32,
    pub session_timeout_ms: u32,
    pub raft_logs_level: LogLevel,
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct RaftServerConfig {
    pub id: KeeperId,
    pub hostname: String,
    pub port: u16,
}

/// Config for an individual Clickhouse Keeper
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct KeeperConfig {
    pub logger: LogConfig,
    pub listen_host: String,
    pub tcp_port: u16,
    pub server_id: KeeperId,
    #[schemars(schema_with = "path_schema")]
    pub log_storage_path: Utf8PathBuf,
    #[schemars(schema_with = "path_schema")]
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
        <enable_reconfiguration>false</enable_reconfiguration>
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

#[allow(unused)]
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
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
