use crate::commands::CommandExecute;
use clap::Parser;
use rocketmq_admin_core::client_adapter::services::controller::ControllerConfigUpdateRequest;
use rocketmq_admin_core::client_adapter::services::controller::ControllerService;
use rocketmq_error::RocketMQResult;

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
    async fn execute(
        &self,
        credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
    ) -> RocketMQResult<()> {
        ControllerService::update_controller_config_by_request_with_credentials(self.request()?, credentials).await?;
        println!("updated controller config successfully");
        Ok(())
    }
}

impl UpdateControllerConfigSubCommand {
    fn request(&self) -> RocketMQResult<ControllerConfigUpdateRequest> {
        ControllerConfigUpdateRequest::try_new(self.controller_address.clone(), self.key.clone(), self.value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_controller_config_sub_command_builds_core_request() {
        let cmd = UpdateControllerConfigSubCommand::try_parse_from([
            "updateControllerConfig",
            "-a",
            " 127.0.0.1:9878 ;127.0.0.2:9878 ",
            "-k",
            " maxElectTimeMs ",
            "-v",
            " 5000 ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();
        assert_eq!(
            request
                .controller_servers()
                .iter()
                .map(|addr| addr.as_str())
                .collect::<Vec<_>>(),
            vec!["127.0.0.1:9878", "127.0.0.2:9878"]
        );
        assert_eq!(request.properties().get("maxElectTimeMs").unwrap().as_str(), "5000");
    }
}
