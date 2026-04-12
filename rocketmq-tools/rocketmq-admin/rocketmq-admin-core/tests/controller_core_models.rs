use rocketmq_admin_core::core::controller::ControllerConfigQueryRequest;
use rocketmq_admin_core::core::controller::ControllerMetadataQueryRequest;

#[test]
fn controller_config_query_request_splits_and_trims_addresses() {
    let request = ControllerConfigQueryRequest::try_new(" 127.0.0.1:9878 ; ; 127.0.0.2:9878 ").unwrap();

    assert_eq!(
        request
            .controller_servers()
            .iter()
            .map(|addr| addr.as_str())
            .collect::<Vec<_>>(),
        vec!["127.0.0.1:9878", "127.0.0.2:9878"]
    );
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn controller_config_query_request_rejects_blank_addresses() {
    assert!(ControllerConfigQueryRequest::try_new(" ; ").is_err());
}

#[test]
fn controller_metadata_query_request_trims_address() {
    let request = ControllerMetadataQueryRequest::try_new(" 127.0.0.1:9878 ").unwrap();

    assert_eq!(request.controller_addr().as_str(), "127.0.0.1:9878");
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn controller_metadata_query_request_rejects_blank_address() {
    assert!(ControllerMetadataQueryRequest::try_new(" ").is_err());
}
