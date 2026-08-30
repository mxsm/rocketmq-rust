// Copyright 2026 The RocketMQ Rust Authors
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

use std::fs::File;
use std::io::Write;

use crate::file_region::FileRegion;
use crate::file_region::FileRegionLease;
use crate::file_region::FileRegionSequence;

use super::*;

struct PlanProcessor {
    plan: Arc<Mutex<Option<RemotingResponse>>>,
}

impl Clone for PlanProcessor {
    fn clone(&self) -> Self {
        Self {
            plan: Arc::clone(&self.plan),
        }
    }
}

impl RequestProcessor for PlanProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        Ok(HandlerOutcome::Reply(
            self.plan
                .lock()
                .expect("plan lock")
                .take()
                .expect("single admitted processor"),
        ))
    }
}

async fn dispatch_plan(plan: RemotingResponse, name: &'static str) -> RemotingResponse {
    let fixture = EmbeddedFixture::new(name);
    let dispatcher = AuthorizedCommandDispatcher::new(
        PlanProcessor {
            plan: Arc::new(Mutex::new(Some(plan))),
        },
        Vec::new(),
        Arc::new(TransportSecurity::secure_enforced(Some(Arc::new(AllowPolicy)), None)),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let outcome = dispatcher
        .dispatch_embedded(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(false).0,
        )
        .await
        .expect("plan dispatch");
    fixture.shutdown().await;
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("expected reply plan")
    };
    plan
}

struct CountingLease {
    file: File,
    drops: Arc<AtomicUsize>,
}

impl FileRegionLease for CountingLease {
    fn file(&self) -> &File {
        &self.file
    }
}

impl Drop for CountingLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn local_handoff_preserves_empty_segments_and_file_region_ownership() {
    let empty = dispatch_plan(
        RemotingResponse::command(RemotingCommand::create_response_command_with_code(80)).expect("empty plan"),
        "embedded-empty-plan",
    )
    .await;
    assert!(matches!(empty.test_body(), ResponseBody::Empty));

    let first = Bytes::from_static(b"first-segment");
    let second = Bytes::from_static(b"second-segment");
    let first_pointer = first.as_ptr();
    let second_pointer = second.as_ptr();
    let segments = dispatch_plan(
        RemotingResponse::segments(
            RemotingCommand::create_response_command_with_code(81),
            vec![first, second],
        )
        .expect("segment plan"),
        "embedded-segment-plan",
    )
    .await;
    let ResponseBody::Segments(segments) = segments.test_body() else {
        panic!("expected segment body")
    };
    assert_eq!(segments[0].as_ptr(), first_pointer);
    assert_eq!(segments[1].as_ptr(), second_pointer);

    let mut file = tempfile::tempfile().expect("temporary region file");
    file.write_all(b"leased-file-region").expect("write region file");
    file.flush().expect("flush region file");
    let drops = Arc::new(AtomicUsize::new(0));
    let lease: Arc<dyn FileRegionLease> = Arc::new(CountingLease {
        file,
        drops: Arc::clone(&drops),
    });
    let region = FileRegion::try_new(lease, 0, 18).expect("file region");
    let regions = FileRegionSequence::try_new(vec![region]).expect("file region sequence");
    let file_plan = dispatch_plan(
        RemotingResponse::file_regions(RemotingCommand::create_response_command_with_code(82), regions)
            .expect("file plan"),
        "embedded-file-plan",
    )
    .await;
    assert!(matches!(file_plan.test_body(), ResponseBody::FileRegions(_)));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(file_plan);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
