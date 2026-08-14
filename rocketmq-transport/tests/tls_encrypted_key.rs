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

#![cfg(feature = "tls")]

use std::fs;

use pkcs8::LineEnding;
use pkcs8::PrivateKeyInfo;
use rocketmq_transport::api::v1::PrivateKeyLoader;

#[test]
fn encrypted_pkcs8_key_requires_the_correct_password_without_leaking_it() {
    let rcgen::CertifiedKey { signing_key, .. } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("generate key");
    let key_der = signing_key.serialize_der();
    let key_info = PrivateKeyInfo::try_from(key_der.as_slice()).expect("parse key");
    let password = "correct horse battery staple";
    let encrypted = key_info
        .encrypt(pkcs8::rand_core::OsRng, password)
        .expect("encrypt key")
        .to_pem("ENCRYPTED PRIVATE KEY", LineEnding::LF)
        .expect("encode PEM");
    let directory = tempfile::tempdir().expect("temp directory");
    let path = directory.path().join("encrypted-key.pem");
    fs::write(&path, encrypted.as_bytes()).expect("write encrypted key");

    let loaded = PrivateKeyLoader::load(&path, "tls.server.keyPath", Some(password)).expect("decrypt key");
    assert!(!loaded.secret_der().is_empty());

    let missing = PrivateKeyLoader::load(&path, "tls.server.keyPath", None).expect_err("password is required");
    assert!(missing.to_string().contains("password is required"));

    let wrong_secret = "definitely-not-the-password";
    let wrong =
        PrivateKeyLoader::load(&path, "tls.server.keyPath", Some(wrong_secret)).expect_err("wrong password must fail");
    let diagnostic = wrong.to_string();
    assert!(diagnostic.contains("decryption failed"));
    assert!(!diagnostic.contains(password));
    assert!(!diagnostic.contains(wrong_secret));
    assert!(!diagnostic.contains("BEGIN ENCRYPTED PRIVATE KEY"));
}
