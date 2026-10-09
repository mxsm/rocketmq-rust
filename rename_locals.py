import os

def replace_in_file(filepath, old, new):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    if old in content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content.replace(old, new))
        print(f"Replaced in {filepath}")

f1 = 'rocketmq-namesrv/src/processor/cluster_test_request_processor/route_lookup.rs'
f2 = 'rocketmq-namesrv/src/processor/cluster_test_request_processor/lookup_cache.rs'

replace_in_file(f1, "let outcome = tokio::select!", "let status = tokio::select!")
replace_in_file(f1, "match outcome {", "match status {")

replace_in_file(f1, "let outcome = resolve_socket_addresses", "let resolution = resolve_socket_addresses")
replace_in_file(f1, "matches!(outcome, NameServerEndpointResolution", "matches!(resolution, NameServerEndpointResolution")

replace_in_file(f1, "response_outcome(response)", "response_resolution(response)")
replace_in_file(f1, "response_outcome(RemotingCommand::", "response_resolution(RemotingCommand::")
replace_in_file(f1, "fn response_outcome(response:", "fn response_resolution(response:")

replace_in_file(f1, "let outcome = lookup", "let resolution = lookup")
replace_in_file(f1, "assert_eq!(outcome, ClusterTestTopicRouteResolution", "assert_eq!(resolution, ClusterTestTopicRouteResolution")
replace_in_file(f1, "assert_eq!(outcome, ClusterTestTopicRouteResolution", "assert_eq!(resolution, ClusterTestTopicRouteResolution")
replace_in_file(f1, "assert_eq!(outcome, ClusterTestTopicRouteResolution", "assert_eq!(resolution, ClusterTestTopicRouteResolution")
replace_in_file(f1, "matches!(outcome, NameServerEndpointResolution", "matches!(resolution, NameServerEndpointResolution")
replace_in_file(f1, "matches!(outcome, NameServerEndpointResolution", "matches!(resolution, NameServerEndpointResolution")

replace_in_file(f2, "let outcome = cache", "let status = cache")
replace_in_file(f2, "matches!(outcome, ClusterTestLookupStatus", "matches!(status, ClusterTestLookupStatus")
replace_in_file(f2, "normal_typed_outcomes", "normal_typed_statuses")
