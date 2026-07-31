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

const GENERAL_HA_CONNECTION: &str = include_str!("../src/ha/general_ha_connection.rs");

#[test]
fn general_ha_connection_has_only_two_constructible_states() {
    let normalized = GENERAL_HA_CONNECTION.replace("\r\n", "\n");
    assert!(
        normalized.contains(concat!(
            "pub enum GeneralHAConnection {\n",
            "    Default(DefaultHAConnection),\n",
            "    AutoSwitch(AutoSwitchHAConnection),\n",
            "}"
        )),
        "GeneralHAConnection must expose exactly the two valid variants"
    );

    for removed_api in [
        "pub fn new()",
        "new_with_default_ha_connection",
        "new_with_auto_switch_ha_connection",
        "set_default_ha_connection",
        "set_auto_switch_ha_connection",
    ] {
        assert!(
            !GENERAL_HA_CONNECTION.contains(removed_api),
            "obsolete HA state API remains: {removed_api}"
        );
    }

    assert!(
        !GENERAL_HA_CONNECTION.contains("panic!("),
        "exhaustive HA delegation must not contain impossible-state panic arms"
    );
}
