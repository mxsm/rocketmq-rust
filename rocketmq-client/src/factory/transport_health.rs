use std::sync::OnceLock;

use tokio::sync::broadcast;

/// A broker transport operation performed for a consumer group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerTransportOperation {
    Heartbeat,
    ConsumerIdList,
}

impl ConsumerTransportOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Heartbeat => "heartbeat",
            Self::ConsumerIdList => "consumer_id_list",
        }
    }
}

/// The result of a broker transport operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerTransportOutcome {
    Success,
    Failure,
}

impl ConsumerTransportOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

/// An application-observable consumer transport result.
///
/// Delivery behavior is intentionally unaffected. Consumers that do not
/// subscribe to this best-effort stream continue to behave exactly as before.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerTransportEvent {
    pub client_id: String,
    pub consumer_group: String,
    pub operation: ConsumerTransportOperation,
    pub outcome: ConsumerTransportOutcome,
}

fn sender() -> &'static broadcast::Sender<ConsumerTransportEvent> {
    static SENDER: OnceLock<broadcast::Sender<ConsumerTransportEvent>> = OnceLock::new();
    SENDER.get_or_init(|| {
        let (sender, _) = broadcast::channel(256);
        sender
    })
}

/// Subscribe to best-effort consumer transport health events in this process.
pub fn subscribe_consumer_transport_events() -> broadcast::Receiver<ConsumerTransportEvent> {
    sender().subscribe()
}

pub(crate) fn emit_consumer_transport_event(event: ConsumerTransportEvent) {
    // Sending is best effort: reporting must never alter broker interaction or
    // fail merely because an application has no observer.
    let _ = sender().send(event);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn observer_receives_best_effort_transport_event() {
        let mut receiver = subscribe_consumer_transport_events();
        let event = ConsumerTransportEvent {
            client_id: "phase4-client".to_owned(),
            consumer_group: "phase4-group".to_owned(),
            operation: ConsumerTransportOperation::Heartbeat,
            outcome: ConsumerTransportOutcome::Failure,
        };
        emit_consumer_transport_event(event.clone());

        assert_eq!(receiver.recv().await.expect("transport event"), event);
    }
}
