use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Parser)]
pub struct UpdateControllerConfigSubCommand {
    #[arg(
        short = 'a',
        long = "controllerAddress",
        value_name = "HOST:PORT",
        required = true,
        help = "Address of the controller to query"
    )]
    controller_address: String,

    #[arg(short = 'k', long = "key", required = true, help = "config key")]
    key: String,

    #[arg(short = 'v', long = "value", required = true, help = "config value")]
    value: String,
}

impl CommandExecute for UpdateControllerConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let controller_address: CheetahString = self.controller_address.as_str().into();

        let key: CheetahString = self.key.as_str().trim().into();

        let value: CheetahString = self.value.as_str().trim().into();

        let mut properties: HashMap<CheetahString, CheetahString> = HashMap::new();

        properties.insert(key, value);

        if controller_address.is_empty() {
            return Err(RocketMQError::Internal("controller address is empty".to_string()));
        }

        let server_array: Vec<CheetahString> = controller_address.split(";").map(|s| CheetahString::from(s)).collect();

        if server_array.is_empty() {
            return Err(RocketMQError::Internal("controller address is empty".to_string()));
        }

        let server_list: Vec<CheetahString> = server_array;

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("AddWritePermSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            match default_mqadmin_ext
                .update_controller_config(properties, server_list)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to update controller config settings: {}", e)))
            {
                Ok(_) => {
                    println!("updated controller config successfully")
                }
                Err(e) => {
                    println!("{e}")
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
