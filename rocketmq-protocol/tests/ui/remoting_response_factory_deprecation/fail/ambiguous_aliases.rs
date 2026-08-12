#![deny(deprecated)]

use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::RemotingCommand;

fn main() {
    let _ = RemotingCommand::create_response_command();
    let _ = RemotingCommand::create_response_command_with_header(GetRouteInfoRequestHeader::new("topic-a", None));
}
