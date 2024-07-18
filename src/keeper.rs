// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
}

/// A client for interacting with keeper instances
pub struct KeeperClient {
    addr: SocketAddr,
}

impl KeeperClient {
    pub fn new(addr: SocketAddr) -> KeeperClient {
        KeeperClient { addr }
    }

    pub async fn config(&self) -> Result<String, KeeperError> {
        self.query("get /keeper/config").await
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
