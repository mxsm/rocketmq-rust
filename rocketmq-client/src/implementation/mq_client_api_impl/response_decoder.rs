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

use super::*;

pub(super) fn consumer_offset_json_from_response(response: &RemotingCommand) -> RocketMQResult<CheetahString> {
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, ToString::to_string)
        ));
    }

    let body = response
        .body()
        .ok_or_else(|| mq_client_err!("get_all_consumer_offset response body is empty"))?;
    let json = std::str::from_utf8(body.as_ref())
        .map_err(|error| mq_client_err!(format!("decode get_all_consumer_offset response body failed: {error}")))?;
    Ok(CheetahString::from_string(json.to_owned()))
}

pub(super) fn notify_result_from_response(response: &RemotingCommand) -> RocketMQResult<NotifyResult> {
    let response_header = response.decode_command_custom_header::<NotificationResponseHeader>()?;
    Ok(NotifyResult::new(response_header.has_msg, response_header.polling_full))
}

pub(super) fn reset_offset_table_from_response(
    response: &RemotingCommand,
) -> RocketMQResult<HashMap<MessageQueue, i64>> {
    if ResponseCode::from(response.code()) == ResponseCode::Success {
        let Some(body) = response.get_body() else {
            return Err(mq_client_err!(
                response.code(),
                response
                    .remark()
                    .map_or_else(|| "reset offset response body is empty".to_string(), |s| s.to_string())
            ));
        };
        let Some(reset_body) = ResetOffsetBody::decode(body.as_ref()) else {
            return Err(mq_client_err!(
                response.code(),
                response
                    .remark()
                    .map_or_else(|| "decode ResetOffsetBody failed".to_string(), |s| s.to_string())
            ));
        };
        return Ok(reset_body.offset_table);
    }

    Err(mq_client_err!(
        response.code(),
        response.remark().map_or("".to_string(), |s| s.to_string())
    ))
}
