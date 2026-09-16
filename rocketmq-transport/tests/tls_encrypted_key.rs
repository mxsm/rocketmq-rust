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
use pkcs8::PrivateKeyInfoRef;
use rocketmq_error::fields;
use rocketmq_error::ErrorContext;
use rocketmq_transport::api::PrivateKeyLoader;

#[test]
fn encrypted_pkcs8_key_requires_the_correct_password_without_leaking_it() {
    let rcgen::CertifiedKey { signing_key, .. } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("generate key");
    let key_der = signing_key.serialize_der();
    let key_info = PrivateKeyInfoRef::try_from(key_der.as_slice()).expect("parse key");
    let password = "correct horse battery staple";
    let encrypted = key_info
        .encrypt(password)
        .expect("encrypt key")
        .to_pem("ENCRYPTED PRIVATE KEY", LineEnding::LF)
        .expect("encode PEM");
    let directory = tempfile::tempdir().expect("temp directory");
    let path = directory.path().join("encrypted-key.pem");
    fs::write(&path, encrypted.as_bytes()).expect("write encrypted key");

    let loaded = PrivateKeyLoader::load(&path, "tls.server.keyPath", Some(password)).expect("decrypt key");
    assert!(!loaded.secret_der().is_empty());

    let missing = PrivateKeyLoader::load(&path, "tls.server.keyPath", None).expect_err("password is required");
    let expected_context = ErrorContext::new()
        .with_text(fields::KEY, "tls.server.keyPath")
        .with_secret_presence(fields::VALUE_PRESENT)
        .with_secret_presence(fields::REASON_PRESENT);
    assert_eq!(missing.descriptor(), &rocketmq_error::CORE_CONFIGURATION_INVALID);
    assert_eq!(missing.context(), &expected_context);

    let wrong_secret = "definitely-not-the-password";
    let wrong =
        PrivateKeyLoader::load(&path, "tls.server.keyPath", Some(wrong_secret)).expect_err("wrong password must fail");
    assert_eq!(wrong.descriptor(), &rocketmq_error::CORE_CONFIGURATION_INVALID);
    assert_eq!(wrong.context(), &expected_context);
    let diagnostic = wrong.to_string();
    assert!(!diagnostic.contains(password));
    assert!(!diagnostic.contains(wrong_secret));
    assert!(!diagnostic.contains("BEGIN ENCRYPTED PRIVATE KEY"));
}
