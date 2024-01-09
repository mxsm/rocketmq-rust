use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::{info, warn};

use crate::{
    codec::remoting_command_codec::RemotingCommandCodec,
    protocol::remoting_command::RemotingCommand,
};

pub struct TokioRemotingServer {
    ip: String,
    port: i32,
    connect_runtime: tokio::runtime::Runtime,
}

type Tx = tokio::sync::mpsc::UnboundedSender<RemotingCommand>;
type Rx = tokio::sync::mpsc::UnboundedReceiver<RemotingCommand>;

struct ConnectionHolder {
    peers: HashMap<SocketAddr, Tx>,
}

impl ConnectionHolder {
    fn new() -> Self {
        ConnectionHolder {
            peers: HashMap::new(),
        }
    }

    async fn process(&mut self, client_addr: SocketAddr, cmd: RemotingCommand) {
        if let Some(tx) = self.peers.get_mut(&client_addr) {
            let opaque = cmd.opaque();
            /*let result = self
                .default_request_processor
                .write()
                .await
                .process_request(cmd);
            //Broker handling compatible with the Java platform.*/
            // let _ = tx.send(result.set_opaque(opaque));
        }
    }
}

struct Connection {
    rx: Rx,
    framed: Framed<TcpStream, RemotingCommandCodec>,
}

impl Connection {
    async fn new(
        state: Arc<tokio::sync::RwLock<ConnectionHolder>>,
        framed: Framed<TcpStream, RemotingCommandCodec>,
    ) -> anyhow::Result<Connection> {
        let addr = framed.get_ref().peer_addr()?;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        state.write().await.peers.insert(addr, tx);
        Ok(Connection { rx, framed })
    }
}

impl TokioRemotingServer {
    pub async fn boot(&self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.ip, self.port);
        let holder = Arc::new(tokio::sync::RwLock::new(ConnectionHolder::new()));
        let listener = TcpListener::bind(&addr).await?;
        loop {
            let (tcp_stream, addr) = listener.accept().await?;
            let holder_clone = holder.clone();
            self.connect_runtime.spawn(async move {
                if let Err(e) = process_connection(holder_clone, tcp_stream, addr).await {
                    tracing::error!("an error occurred; error = {:?}", e)
                }
            });
        }
    }
}

async fn process_connection(
    holder: Arc<tokio::sync::RwLock<ConnectionHolder>>,
    tcp_stream: TcpStream,
    addr: SocketAddr,
) -> anyhow::Result<(), anyhow::Error> {
    let framed = Framed::new(tcp_stream, RemotingCommandCodec::new());
    let mut conn = Connection::new(holder.clone(), framed).await?;
    loop {
        tokio::select! {
            result = conn.framed.next() => match result {
                // A message was received from the current user, we should
                // broadcast this message to the other users.
                Some(Ok(msg)) => {
                    //let mut state1 = state.lock().await;
                    info!("{}",serde_json::to_string(&msg).unwrap());
                    let mut state = holder.write().await;
                    state.process(addr, msg).await;
                }
                // An error occurred.
                Some(Err(e)) => {
                    tracing::error!(
                        "an error occurred while processing messages for error = {:?}",e);
                }
                // The stream has been exhausted.
                None => break,
            },
            // A message was received from a peer. Send it to the current user.
            Some(msg) = conn.rx.recv() => {
                conn.framed.send(msg).await?;
            }
        }
    }

    {
        //remove disconnected client connection
        let mut state = holder.write().await;
        state.peers.remove(&addr);
        warn!("remove client [IP={}] connection name server.", addr);
    }
    Ok(())
}
