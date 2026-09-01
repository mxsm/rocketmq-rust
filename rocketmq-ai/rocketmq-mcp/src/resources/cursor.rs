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

use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::Validation;
use serde::Deserialize;
use serde::Serialize;

const TOKEN_PREFIX: &str = "rmq-discovery-v1.";
const TOKEN_TYPE: &str = "RMQ-DISCOVERY";
const FORMAT_VERSION: u8 = 1;
const KEY_BYTES: usize = 32;
const MAX_TOKEN_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiscoverySurface {
    Resources,
    Templates,
}

#[derive(Clone)]
pub(crate) struct DiscoveryCursorCodec {
    signing_key: EncodingKey,
    generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorClaims {
    version: u8,
    surface: DiscoverySurface,
    offset: u64,
    generation: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DiscoveryCursorError {
    #[error("operating system random source is unavailable")]
    Random(#[source] std::io::Error),
    #[error("discovery cursor is invalid")]
    Invalid,
}

impl DiscoveryCursorCodec {
    pub(crate) fn new() -> Result<Self, DiscoveryCursorError> {
        let mut material = [0u8; KEY_BYTES + std::mem::size_of::<u64>()];
        fill_random(&mut material).map_err(DiscoveryCursorError::Random)?;
        let (key, generation) = material.split_at(KEY_BYTES);
        let generation = u64::from_be_bytes(generation.try_into().map_err(|_| DiscoveryCursorError::Invalid)?);
        Ok(Self::from_material(key, generation))
    }

    fn from_material(key: &[u8], generation: u64) -> Self {
        Self {
            signing_key: EncodingKey::from_secret(key),
            generation,
        }
    }

    pub(crate) fn seal(
        &self,
        surface: DiscoverySurface,
        offset: usize,
        canonical_auth_claims: &[u8],
    ) -> Result<String, DiscoveryCursorError> {
        let claims = CursorClaims {
            version: FORMAT_VERSION,
            surface,
            offset: u64::try_from(offset).map_err(|_| DiscoveryCursorError::Invalid)?,
            generation: self.generation,
        };
        let key = self.principal_key(canonical_auth_claims)?;
        let token = jsonwebtoken::encode(&token_header(), &claims, &key).map_err(|_| DiscoveryCursorError::Invalid)?;
        Ok(format!("{TOKEN_PREFIX}{token}"))
    }

    pub(crate) fn open(
        &self,
        surface: DiscoverySurface,
        token: &str,
        canonical_auth_claims: &[u8],
    ) -> Result<usize, DiscoveryCursorError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(DiscoveryCursorError::Invalid);
        }
        let token = token.strip_prefix(TOKEN_PREFIX).ok_or(DiscoveryCursorError::Invalid)?;
        if token.matches('.').count() != 2 {
            return Err(DiscoveryCursorError::Invalid);
        }
        let key = self.principal_verification_key(canonical_auth_claims)?;
        let mut validation = Validation::new(Algorithm::HS256);
        validation.required_spec_claims.clear();
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.validate_aud = false;
        let decoded = jsonwebtoken::decode::<CursorClaims>(token, &key, &validation)
            .map_err(|_| DiscoveryCursorError::Invalid)?;
        if decoded.header.typ.as_deref() != Some(TOKEN_TYPE)
            || decoded.claims.version != FORMAT_VERSION
            || decoded.claims.surface != surface
            || decoded.claims.generation != self.generation
        {
            return Err(DiscoveryCursorError::Invalid);
        }
        let canonical = jsonwebtoken::encode(
            &token_header(),
            &decoded.claims,
            &self.principal_key(canonical_auth_claims)?,
        )
        .map_err(|_| DiscoveryCursorError::Invalid)?;
        if canonical != token {
            return Err(DiscoveryCursorError::Invalid);
        }
        usize::try_from(decoded.claims.offset).map_err(|_| DiscoveryCursorError::Invalid)
    }

    fn principal_key(&self, canonical_auth_claims: &[u8]) -> Result<EncodingKey, DiscoveryCursorError> {
        let derived = jsonwebtoken::crypto::sign(canonical_auth_claims, &self.signing_key, Algorithm::HS256)
            .map_err(|_| DiscoveryCursorError::Invalid)?;
        Ok(EncodingKey::from_secret(derived.as_bytes()))
    }

    fn principal_verification_key(&self, canonical_auth_claims: &[u8]) -> Result<DecodingKey, DiscoveryCursorError> {
        let derived = jsonwebtoken::crypto::sign(canonical_auth_claims, &self.signing_key, Algorithm::HS256)
            .map_err(|_| DiscoveryCursorError::Invalid)?;
        Ok(DecodingKey::from_secret(derived.as_bytes()))
    }
}

fn token_header() -> Header {
    let mut header = Header::new(Algorithm::HS256);
    header.typ = Some(TOKEN_TYPE.to_string());
    header
}

#[cfg(windows)]
fn fill_random(output: &mut [u8]) -> std::io::Result<()> {
    const BCRYPT_USE_SYSTEM_PREFERRED_RNG: u32 = 0x0000_0002;

    #[link(name = "bcrypt")]
    unsafe extern "system" {
        fn BCryptGenRandom(algorithm: *mut std::ffi::c_void, output: *mut u8, len: u32, flags: u32) -> i32;
    }

    let len = u32::try_from(output.len())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "random request is too large"))?;
    // SAFETY: `output` is a writable allocation of exactly `len` bytes, the
    // algorithm handle is null as required by BCRYPT_USE_SYSTEM_PREFERRED_RNG,
    // and BCryptGenRandom does not retain the pointer after returning.
    let status = unsafe {
        BCryptGenRandom(
            std::ptr::null_mut(),
            output.as_mut_ptr(),
            len,
            BCRYPT_USE_SYSTEM_PREFERRED_RNG,
        )
    };
    if status >= 0 {
        Ok(())
    } else {
        Err(std::io::Error::other("BCryptGenRandom failed"))
    }
}

#[cfg(unix)]
fn fill_random(output: &mut [u8]) -> std::io::Result<()> {
    use std::io::Read;

    std::fs::File::open("/dev/urandom")?.read_exact(output)
}

#[cfg(not(any(windows, unix)))]
fn fill_random(_output: &mut [u8]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "operating system random source is unsupported",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codec(generation: u64) -> DiscoveryCursorCodec {
        DiscoveryCursorCodec::from_material(&[0x5a; KEY_BYTES], generation)
    }

    #[test]
    fn cursor_is_instance_principal_surface_and_generation_bound() {
        let first = codec(7);
        let token = first
            .seal(DiscoverySurface::Resources, 50, b"principal-a-standard")
            .unwrap();
        assert_eq!(
            first
                .open(DiscoverySurface::Resources, &token, b"principal-a-standard")
                .unwrap(),
            50
        );
        assert!(first
            .open(DiscoverySurface::Templates, &token, b"principal-a-standard")
            .is_err());
        assert!(first
            .open(DiscoverySurface::Resources, &token, b"principal-b-standard")
            .is_err());
        assert!(codec(8)
            .open(DiscoverySurface::Resources, &token, b"principal-a-standard")
            .is_err());
        let replacement = DiscoveryCursorCodec::from_material(&[0xa5; KEY_BYTES], 7);
        assert!(replacement
            .open(DiscoverySurface::Resources, &token, b"principal-a-standard")
            .is_err());
    }

    #[test]
    fn cursor_rejects_prefix_claim_signature_splices_and_every_single_byte_tamper() {
        let codec = codec(7);
        let token = codec.seal(DiscoverySurface::Resources, 50, b"principal-a").unwrap();
        let other = codec.seal(DiscoverySurface::Resources, 75, b"principal-b").unwrap();
        let token_parts = token.split('.').collect::<Vec<_>>();
        let other_parts = other.split('.').collect::<Vec<_>>();
        for forged in [
            token.replacen(TOKEN_PREFIX, "rmq-discovery-v2.", 1),
            format!(
                "{}.{}.{}.{}",
                token_parts[0], token_parts[1], other_parts[2], token_parts[3]
            ),
            format!(
                "{}.{}.{}.{}",
                token_parts[0], token_parts[1], token_parts[2], other_parts[3]
            ),
        ] {
            assert!(codec
                .open(DiscoverySurface::Resources, &forged, b"principal-a")
                .is_err());
        }

        for index in 0..token.len() {
            let mut tampered = token.as_bytes().to_vec();
            tampered[index] = match tampered[index] {
                b'a' => b'b',
                _ => b'a',
            };
            let tampered = String::from_utf8(tampered).unwrap();
            assert!(
                codec
                    .open(DiscoverySurface::Resources, &tampered, b"principal-a")
                    .is_err(),
                "index={index}"
            );
        }
    }
}
