#![deny(deprecated)]

use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::RemotingCommand;

fn main() {
    let mut command = RemotingCommand::create_request_command(1, GetRouteInfoRequestHeader::new("topic-a", None));
    let _ = command.read_custom_header_ref_unchecked::<GetRouteInfoRequestHeader>();
    let _ = command.read_custom_header_mut_unchecked::<GetRouteInfoRequestHeader>();
}
