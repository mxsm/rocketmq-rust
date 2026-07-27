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

use crate::DataClass;
use crate::ProviderCapability;
use crate::ProviderError;
use crate::ProviderErrorCode;
use crate::ProviderProfile;

const MAX_MODEL_FAMILY_CHARS: usize = 128;

/// Produces the stable identity used to enforce heterogeneous Critic routing.
///
/// Endpoint, region, model revision, and profile aliases are deliberately not
/// part of this identity. Separator and case differences therefore cannot make
/// two deployments of the same model family appear heterogeneous.
///
/// # Errors
///
/// Returns [`ProviderErrorCode::ProfileInvalid`] when the family is empty,
/// contains control characters, or exceeds the bounded identity length.
pub fn normalize_model_family(value: &str) -> Result<String, ProviderError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > MAX_MODEL_FAMILY_CHARS || value.chars().any(char::is_control) {
        return Err(ProviderError::new(
            ProviderErrorCode::ProfileInvalid,
            "model family must be a bounded non-empty identity",
        ));
    }

    let mut normalized = String::with_capacity(value.len());
    let mut pending_separator = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_alphanumeric() {
            if pending_separator && !normalized.is_empty() {
                normalized.push('-');
            }
            normalized.push(character);
            pending_separator = false;
        } else {
            pending_separator = true;
        }
    }
    if normalized.is_empty() {
        return Err(ProviderError::new(
            ProviderErrorCode::ProfileInvalid,
            "model family must contain at least one letter or number",
        ));
    }
    Ok(normalized)
}

/// Returns routable structured-output profiles from a different model family.
///
/// The result is ordered by configured priority and stable profile identifier.
/// Every returned profile is independently normalized and compared with the
/// actual family recorded for the primary invocation.
///
/// # Errors
///
/// Returns [`ProviderErrorCode::ProfileInvalid`] when either the primary or a
/// candidate family cannot be normalized.
pub fn heterogeneous_critic_profiles<'a>(
    primary_model_family: &str,
    profiles: &'a [ProviderProfile],
    data_class: DataClass,
) -> Result<Vec<&'a ProviderProfile>, ProviderError> {
    let primary = normalize_model_family(primary_model_family)?;
    let mut candidates = Vec::new();
    for profile in profiles {
        let candidate = normalize_model_family(&profile.model_family)?;
        if candidate == primary
            || !profile.health.routable()
            || !profile.allowed_data_classes.contains(&data_class)
            || !profile.capabilities.supported.contains(&ProviderCapability::Chat)
            || !profile.capabilities.supported.contains(&ProviderCapability::Text)
            || !profile.capabilities.supported.contains(&ProviderCapability::JsonSchema)
        {
            continue;
        }
        candidates.push(profile);
    }
    candidates.sort_by_key(|profile| (profile.priority, profile.id.as_str()));
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_collapses_alias_formatting_without_endpoint_identity() {
        assert_eq!(normalize_model_family(" GPT_4.1 ").expect("family"), "gpt-4-1");
        assert_eq!(normalize_model_family("gpt---4 1").expect("family"), "gpt-4-1");
        assert!(normalize_model_family("\n").is_err());
        assert!(normalize_model_family(&"x".repeat(MAX_MODEL_FAMILY_CHARS + 1)).is_err());
    }

    #[test]
    fn same_family_aliases_and_endpoints_never_satisfy_heterogeneity() {
        let mut profiles = crate::builtin_provider_profiles();
        let primary = profiles
            .iter()
            .find(|profile| profile.id == "openai")
            .expect("OpenAI fixture")
            .clone();
        let azure = profiles
            .iter_mut()
            .find(|profile| profile.id == "azure-openai")
            .expect("Azure OpenAI fixture");
        azure.model_family = "GPT".to_owned();
        azure.endpoint_instance = "another-region-and-endpoint".to_owned();
        azure.priority = 0;

        let candidates = heterogeneous_critic_profiles(&primary.model_family, &profiles, DataClass::Internal)
            .expect("critic candidates");

        assert!(!candidates.iter().any(|profile| profile.id == "openai"));
        assert!(!candidates.iter().any(|profile| profile.id == "azure-openai"));
        assert!(candidates.iter().any(|profile| profile.model_family != "gpt"));
    }

    #[test]
    fn candidates_are_bounded_by_data_class_capability_health_and_priority() {
        let mut profiles = crate::builtin_provider_profiles();
        let deepseek = profiles
            .iter_mut()
            .find(|profile| profile.id == "deepseek")
            .expect("DeepSeek fixture");
        deepseek.priority = 1;
        let unavailable_family = deepseek.model_family.clone();
        deepseek.health = crate::ProviderHealth::Unavailable;

        let candidates =
            heterogeneous_critic_profiles("gpt", &profiles, DataClass::Restricted).expect("restricted candidates");

        assert!(
            candidates
                .iter()
                .all(|profile| profile.allowed_data_classes.contains(&DataClass::Restricted))
        );
        assert!(
            !candidates
                .iter()
                .any(|profile| profile.model_family == unavailable_family)
        );
        assert!(
            candidates
                .windows(2)
                .all(|pair| { (pair[0].priority, pair[0].id.as_str()) <= (pair[1].priority, pair[1].id.as_str()) })
        );
    }
}
