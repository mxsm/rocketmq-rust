import os

src_dir = 'rocketmq-namesrv/src'

replacements = {
    'EndpointResolutionOutcome': 'NameServerEndpointResolution',
    'RouteLookupOutcome': 'ClusterTestLookupStatus',
    'ClusterTestTopicRouteOutcome': 'ClusterTestTopicRouteResolution'
}

for root, dirs, files in os.walk(src_dir):
    for file in files:
        if file.endswith('.rs'):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            new_content = content
            for old, new in replacements.items():
                new_content = new_content.replace(old, new)
                
            if new_content != content:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f"Updated {filepath}")
