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

use std::any::Any;
use std::collections::HashMap;

use cheetah_string::CheetahString;

use crate::authentication::AsAny;
use crate::authorization::context::authentication_context::AuthenticationContext;

#[derive(Debug, Default)]
pub struct BaseAuthenticationContext {
    channel_id: Option<CheetahString>,

    rpc_code: Option<CheetahString>,

    ext_info: HashMap<CheetahString, Box<dyn Any + Send + Sync>>,
}

impl BaseAuthenticationContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn channel_id(&self) -> Option<&CheetahString> {
        self.channel_id.as_ref()
    }

    pub fn set_channel_id(&mut self, channel_id: Option<CheetahString>) {
        self.channel_id = channel_id;
    }

    pub fn rpc_code(&self) -> Option<&CheetahString> {
        self.rpc_code.as_ref()
    }

    pub fn set_rpc_code(&mut self, rpc_code: Option<CheetahString>) {
        self.rpc_code = rpc_code;
    }
}

impl BaseAuthenticationContext {
    pub fn get_ext_info<T: 'static>(&self, key: &CheetahString) -> Option<&T> {
        self.ext_info.get(key).and_then(|v| v.downcast_ref::<T>())
    }
    pub fn has_ext_info(&self, key: &CheetahString) -> bool {
        self.ext_info.contains_key(key)
    }
    pub fn ext_info(&self) -> &HashMap<CheetahString, Box<dyn Any + Send + Sync>> {
        &self.ext_info
    }

    pub fn ext_info_mut(&mut self) -> &mut HashMap<CheetahString, Box<dyn Any + Send + Sync>> {
        &mut self.ext_info
    }
}

impl BaseAuthenticationContext {
    pub fn set_ext_info<T>(&mut self, key: CheetahString, value: T)
    where
        T: Any + Send + Sync,
    {
        self.ext_info.insert(key, Box::new(value));
    }
}

impl AuthenticationContext for BaseAuthenticationContext {}

impl AsAny for BaseAuthenticationContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use crate::authentication::context::base_authentication_context::BaseAuthenticationContext;
    use crate::authentication::AsAny;

    #[test]
    fn new_base_authentication_context() {
        let base_authentication_context = BaseAuthenticationContext::new();

        assert!(base_authentication_context.channel_id().is_none());
        assert!(base_authentication_context.rpc_code().is_none());
        assert!(base_authentication_context.ext_info().is_empty());
    }

    #[test]
    fn set_and_get_channel_and_rpc() {
        let mut base_authentication_context = BaseAuthenticationContext::new();
        base_authentication_context.set_channel_id(Some(CheetahString::from("channel-123")));
        base_authentication_context.set_rpc_code(Some(CheetahString::from("rpc-456")));

        assert_eq!(
            Some(&CheetahString::from("channel-123")),
            base_authentication_context.channel_id()
        );
        assert_eq!(
            Some(&CheetahString::from("rpc-456")),
            base_authentication_context.rpc_code()
        );
    }

    #[test]
    fn set_get_and_has_ext_info() {
        let mut base_authentication_context = BaseAuthenticationContext::new();
        base_authentication_context.set_ext_info(CheetahString::from("info-1"), 123);

        assert!(base_authentication_context.has_ext_info(&CheetahString::from("info-1")));
        assert_eq!(
            Some(123),
            base_authentication_context
                .get_ext_info::<i32>(&CheetahString::from("info-1"))
                .copied()
        );
    }

    #[test]
    fn ext_info_nonexistent_key_returns_none() {
        let base_authentication_context = BaseAuthenticationContext::new();

        assert!(!base_authentication_context.has_ext_info(&CheetahString::from("nonexistent")));
        assert_eq!(
            None,
            base_authentication_context.get_ext_info::<i32>(&CheetahString::from("nonexistent"))
        );
    }

    #[test]
    fn ext_info_wrong_type_returns_none() {
        let mut base_authentication_context = BaseAuthenticationContext::new();
        base_authentication_context.set_ext_info(CheetahString::from("info-2"), CheetahString::from("xyz"));

        assert_eq!(
            None,
            base_authentication_context
                .get_ext_info::<u32>(&CheetahString::from("info-2"))
                .copied()
        );
    }

    #[test]
    fn ext_info_mut_can_modify() {
        let mut base_authentication_context = BaseAuthenticationContext::new();
        base_authentication_context
            .ext_info_mut()
            .insert(CheetahString::from("info-3"), Box::new(64u32));

        assert_eq!(
            Some(64u32),
            base_authentication_context
                .get_ext_info::<u32>(&CheetahString::from("info-3"))
                .copied()
        );
    }

    #[test]
    fn as_any_downcasts_work() {
        let mut base_authentication_context = BaseAuthenticationContext::new();

        let any_ref = base_authentication_context.as_any();
        assert!(any_ref.downcast_ref::<BaseAuthenticationContext>().is_some());

        let any_mut_ref = base_authentication_context.as_any_mut();
        assert!(any_mut_ref.downcast_mut::<BaseAuthenticationContext>().is_some());
    }
}
