use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
#[derive(Default)]
pub struct NotifyMinBrokerChangeIdProcessor {}

impl NotifyMinBrokerChangeIdProcessor {
    pub async fn process_request(
        &mut self,
        _channel: Channel,
        ctx: ConnectionHandlerContext,
        mut request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("CheckClientConfig")
    }
}
