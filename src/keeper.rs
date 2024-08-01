// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::Stdio;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

#[derive(Error, Debug)]
pub enum KeeperError {
    #[error("no config present")]
    NoConfig,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("unexpected response")]
    UnexpectedResponse,
}

#[derive(Debug, Clone)]
pub struct KeeperConfig {
    pub addr: String,
}

/// A client for interacting with keeper instances
#[derive(Debug, Clone)]
pub struct KeeperClient {
    addr: SocketAddr,
}

impl KeeperClient {
    pub fn new(addr: SocketAddr) -> KeeperClient {
        KeeperClient { addr }
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub async fn config(
        &self,
    ) -> Result<BTreeMap<u64, KeeperConfig>, KeeperError> {
        let output = self.query("get /keeper/config").await?;
        let mut config = BTreeMap::new();
        for line in output.lines() {
            let s = line
                .strip_prefix("server.")
                .ok_or(KeeperError::UnexpectedResponse)?;
            let mut iter = s.split('=');
            let id = iter.next().ok_or(KeeperError::UnexpectedResponse)?;
            let rest = iter.next().ok_or(KeeperError::UnexpectedResponse)?;
            let addr = rest
                .split(';')
                .next()
                .ok_or(KeeperError::UnexpectedResponse)?;
            let id = id
                .parse::<u64>()
                .map_err(|_| KeeperError::UnexpectedResponse)?;
            config.insert(id, KeeperConfig { addr: addr.to_string() });
        }
        Ok(config)
    }

    async fn query(&self, query: &str) -> Result<String, KeeperError> {
        let mut child = Command::new("clickhouse")
            .arg("keeper-client")
            .arg("--port")
            .arg(self.addr.port().to_string())
            .arg("--query")
            .arg(query)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;

        let mut stdout = child.stdout.take().unwrap();
        let mut output = String::new();
        stdout.read_to_string(&mut output).await?;
        Ok(output)
    }
}
