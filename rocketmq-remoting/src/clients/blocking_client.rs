// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub struct BlockingClient {
    /// A `current_thread` runtime for executing operations on the asynchronous
    /// client in a blocking manner.
    rt: tokio::runtime::Runtime,
}

#[allow(clippy::needless_doctest_main)]
impl BlockingClient {
    /// Establish a connection with the rocketmq remoting_server located at `addr`.
    ///
    /// `addr` may be any type that can be asynchronously converted to a
    /// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
    /// trait is the Tokio version and not the `std` version.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rocketmq_remoting::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let client = match BlockingClient::connect("localhost:6379") {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("failed to establish connection"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    pub fn connect<T: tokio::net::ToSocketAddrs>(_addr: T) -> anyhow::Result<BlockingClient> {
        /*let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let inner = rt.block_on(crate::clients::Client::connect(addr))?;
        Ok(BlockingClient { inner, rt })*/
        unimplemented!("BlockingClient::connect")
    }

    /*    pub fn invoke_oneway(
        &mut self,
        request: RemotingCommand,
        timeout: Duration,
    ) -> crate::Result<()> {
        match self
            .rt
            .block_on(tokio::time::timeout(timeout, self.inner.send(request)))
        {
            Ok(value) => value,
            Err(err) => Err(crate::error::Error::Elapsed(err)),
        }
    }*/
}
