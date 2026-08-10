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

use std::fs;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use rocketmq_store_local::mapped_file::file::MappedFileStorage;

#[test]
fn same_path_replacement_cannot_rebind_the_retained_physical_owner() {
    let root = tempfile::tempdir().expect("temporary segment directory");
    let canonical = root.path().join("00000000000000000000");
    let displaced = root.path().join("retired-original");
    let (mut storage, _) = MappedFileStorage::open(canonical.clone(), 32).expect("open original segment");
    storage
        .with_file(|file| {
            let mut file = file;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(b"original-incarnation")?;
            file.sync_all()
        })
        .expect("the original owner is attached")
        .expect("write the original owner");

    fs::rename(&canonical, &displaced).expect("move original namespace entry aside");
    fs::write(&canonical, b"replacement-incarnation..........").expect("install same-path replacement");

    let error = storage
        .reopen()
        .expect_err("same path must not authorize a different physical file");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(storage.path(), canonical);

    let original = storage
        .with_file(|file| {
            let mut file = file;
            let mut bytes = vec![0; b"original-incarnation".len()];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut bytes)?;
            Ok::<_, io::Error>(bytes)
        })
        .expect("the retained owner is still attached")
        .expect("read the original incarnation through the retained owner");
    assert_eq!(original, b"original-incarnation");
    assert_eq!(
        &fs::read(&canonical).expect("replacement remains")[..b"replacement-incarnation".len()],
        b"replacement-incarnation"
    );
}
