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

use std::error::Error;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use tokio::sync::Notify;

use crate::producer::request_callback::RequestCallbackFn;

type ResponseMessage = Box<dyn MessageTrait + Send>;
type RequestCause = Arc<dyn Error + Send + Sync>;

pub struct RequestResponseFuture {
    correlation_id: CheetahString,
    request_callback: Option<RequestCallbackFn>,
    begin_timestamp: Instant,
    request_msg: Option<Message>,
    timeout_millis: u64,
    notify: Arc<Notify>,
    response_msg: Mutex<Option<ResponseMessage>>,
    send_request_ok: AtomicBool,
    cause: Mutex<Option<RequestCause>>,
}

impl RequestResponseFuture {
    pub fn new(
        correlation_id: CheetahString,
        timeout_millis: u64,
        request_callback: Option<RequestCallbackFn>,
    ) -> Self {
        Self {
            correlation_id,
            request_callback,
            begin_timestamp: Instant::now(),
            request_msg: None,
            timeout_millis,
            notify: Arc::new(Notify::new()),
            response_msg: Mutex::new(None),
            send_request_ok: AtomicBool::new(true),
            cause: Mutex::new(None),
        }
    }

    pub fn execute_request_callback(&self) {
        if let Some(ref callback) = self.request_callback {
            let send_request_ok = self.send_request_ok.load(Ordering::Acquire);
            let cause = self.get_cause();
            if send_request_ok && cause.is_none() {
                let response_msg = self
                    .response_msg
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                callback(
                    response_msg.as_deref().map(|message| message as &dyn MessageTrait),
                    None,
                );
            } else {
                callback(
                    None,
                    cause.as_ref().map(|cause| cause.as_ref() as &dyn std::error::Error),
                );
            }
        }
    }

    pub fn is_timeout(&self) -> bool {
        self.begin_timestamp.elapsed() > Duration::from_millis(self.timeout_millis)
    }

    pub async fn wait_response_message(&self, timeout: Duration) -> Option<Box<dyn MessageTrait + Send>> {
        match tokio::time::timeout(timeout, self.notify.notified()).await {
            Ok(_) => self.get_response_msg(),
            Err(error) => {
                self.set_cause(Box::new(error));
                None
            }
        }
    }

    pub fn put_response_message(&self, response_msg: Option<Box<dyn MessageTrait + Send>>) {
        let mut stored_response = self
            .response_msg
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *stored_response = response_msg;
        self.notify.notify_waiters();
    }

    // Getters and setters
    pub fn get_correlation_id(&self) -> &str {
        &self.correlation_id
    }

    pub fn get_timeout_millis(&self) -> u64 {
        self.timeout_millis
    }

    pub fn set_timeout_millis(&mut self, timeout_millis: u64) {
        self.timeout_millis = timeout_millis;
    }

    pub fn get_request_callback(&self) -> Option<&RequestCallbackFn> {
        self.request_callback.as_ref()
    }

    pub fn on_success(&self) {
        if let Some(callback) = &self.request_callback {
            let response_msg = self
                .response_msg
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            callback(
                response_msg.as_deref().map(|message| message as &dyn MessageTrait),
                None,
            );
        }
    }

    pub fn get_begin_timestamp(&self) -> Instant {
        self.begin_timestamp
    }

    pub fn get_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    #[inline]
    pub fn get_response_msg(&self) -> Option<Box<dyn MessageTrait + Send>> {
        self.response_msg
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    pub fn set_response_msg(&self, response_msg: Box<dyn MessageTrait + Send>) {
        self.put_response_message(Some(response_msg));
    }

    pub async fn is_send_request_ok(&self) -> bool {
        self.send_request_ok.load(Ordering::Acquire)
    }

    pub fn set_send_request_ok(&self, send_request_ok: bool) {
        self.send_request_ok.store(send_request_ok, Ordering::Release)
    }

    pub fn get_request_msg(&self) -> Option<&Message> {
        self.request_msg.as_ref()
    }

    pub fn get_cause(&self) -> Option<RequestCause> {
        self.cause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn set_cause(&self, cause: Box<dyn Error + Send + Sync>) {
        let mut stored_cause = self.cause.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *stored_cause = Some(Arc::from(cause));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use bytes::Bytes;

    use super::*;

    fn response_message() -> Box<dyn MessageTrait + Send> {
        Box::new(
            Message::builder()
                .topic("reply_topic")
                .body(Bytes::from_static(b"ok"))
                .build_unchecked(),
        )
    }

    #[tokio::test]
    async fn default_send_request_ok_matches_java() {
        let future = RequestResponseFuture::new("corr".into(), 3_000, None);
        assert!(future.is_send_request_ok().await);

        future.set_send_request_ok(false);
        assert!(!future.is_send_request_ok().await);

        future.set_send_request_ok(true);
        assert!(future.is_send_request_ok().await);
    }

    #[test]
    fn success_callback_accepts_missing_response_without_panic() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);
        let callback: RequestCallbackFn = Arc::new(move |response, error| {
            assert!(response.is_none());
            assert!(error.is_none());
            calls_inner.fetch_add(1, Ordering::SeqCst);
        });

        let future = RequestResponseFuture::new("corr".into(), 3_000, Some(callback));
        future.execute_request_callback();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn callback_can_read_response_more_than_once_without_consuming_it() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);
        let callback: RequestCallbackFn = Arc::new(move |response, error| {
            assert!(response.is_some());
            assert!(error.is_none());
            calls_inner.fetch_add(1, Ordering::SeqCst);
        });

        let future = RequestResponseFuture::new("corr".into(), 3_000, Some(callback));
        future.put_response_message(Some(response_message()));

        future.on_success();
        future.execute_request_callback();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(future.get_response_msg().is_some());
        assert!(future.get_response_msg().is_none());
    }

    #[test]
    fn failure_callback_passes_none_when_cause_is_absent_like_java_null() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);
        let callback: RequestCallbackFn = Arc::new(move |response, error| {
            assert!(response.is_none());
            assert!(error.is_none());
            calls_inner.fetch_add(1, Ordering::SeqCst);
        });

        let future = RequestResponseFuture::new("corr".into(), 3_000, Some(callback));
        future.set_send_request_ok(false);
        future.execute_request_callback();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
