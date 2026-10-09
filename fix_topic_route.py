import os

def replace_in_file(filepath, old, new):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    if old in content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content.replace(old, new))
        print(f"Replaced in {filepath}")

files = [
    'rocketmq-namesrv/src/processor/cluster_test_request_processor.rs',
    'rocketmq-namesrv/src/bootstrap.rs'
]

for f in files:
    replace_in_file(f, "TopicClusterTestLookupStatus", "TopicRouteLookupOutcome")
