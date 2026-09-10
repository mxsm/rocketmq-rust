#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate isolated loopback cluster configs; never build, launch, or delete."""

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import re
import socket
import sys
import tomllib


PROFILES = (
    "dev-single", "dev-ha", "dev-ha-2m2s", "proxy-cluster", "proxy-local", "controller-ha",
)
HEADER = "# Generated local development configuration; keep per-node data isolated.\n"


def quoted(value):
    # JSON basic strings are valid TOML for these generated paths and identifiers.
    return json.dumps(str(value), ensure_ascii=False)


def table(values):
    def scalar(value):
        if isinstance(value, bool):
            return str(value).lower()
        return str(value) if isinstance(value, int) else quoted(value)
    return "\n".join(f"{key} = {scalar(value)}" for key, value in values.items()) + "\n"


def build_plan(repo, output, name, profile, offset=0, with_proxy=False,
               replication=None, proxy_remoting=False):
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", name):
        raise ValueError("name must be 1-40 lowercase letters, digits or hyphens, starting with a letter")
    if name in {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)),
                *(f"lpt{i}" for i in range(1, 10))}:
        raise ValueError("name is reserved on Windows")
    if profile not in PROFILES:
        raise ValueError("unknown profile")
    ordinary_ha = profile in {"dev-ha", "dev-ha-2m2s"}
    controller = profile == "controller-ha"
    local = profile == "proxy-local"
    proxy = profile in {"proxy-local", "proxy-cluster"} or with_proxy
    if with_proxy and profile in {"proxy-cluster", "proxy-local"}:
        raise ValueError("this profile already includes a Proxy")
    if replication is not None and (not ordinary_ha or replication not in {"sync", "async"}):
        raise ValueError("replication selection requires an ordinary HA profile")
    if proxy_remoting and not proxy:
        raise ValueError("proxy-remoting requires a Proxy")
    if offset < 0:
        raise ValueError("port-offset must not be negative")

    repo, output = Path(repo).resolve(), Path(output).resolve()
    cluster_name = f"Local-{name}"
    files, nodes, builds, ports = {}, [], [], []

    def port(base):
        value = base + offset
        if not 1 <= value <= 65535:
            raise ValueError(f"port-offset produces invalid TCP port {value}")
        return value

    def endpoint(base):
        return f"127.0.0.1:{port(base)}"

    def home(node):
        return (output / "nodes" / node).as_posix()

    def add_node(node, service, content, listeners, extra_env=None):
        config_path = output / "conf" / f"{node}.toml"
        tomllib.loads(content)
        files[f"conf/{node}.toml"] = HEADER + content
        probe = endpoint(18000 + len(nodes))
        bindings = {**listeners, "health": probe}
        for purpose, address in bindings.items():
            ports.append({"node": node, "purpose": purpose, "address": address})
        env = {
            "ROCKETMQ_HOME": home(node),
            "ROCKETMQ_SECURITY_PROFILE": "development-insecure-loopback",
            "ROCKETMQ_HEALTH_BIND_ADDR": probe,
            "ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS": "45",
            "RUST_LOG": "info",
            **(extra_env or {}),
        }
        nodes.append({
            "id": node, "service": service, "binary": f"rocketmq-{service}-rust",
            "argv": ["-c", config_path.as_posix()],
            "print_config_flag": "--printConfig" if service == "proxy" else "-p",
            "cwd": home(node), "env": env, "ports": bindings,
            "stdout": (output / "logs" / f"{node}.out.log").as_posix(),
            "stderr": (output / "logs" / f"{node}.err.log").as_posix(),
            "ready_url": f"http://{probe}/readyz",
        })

    namesrv_count = 0 if local else (2 if ordinary_ha or controller else 1)
    namesrv_addr = ";".join(endpoint(9876 + i * 10) for i in range(namesrv_count))
    for i in range(namesrv_count):
        node = f"namesrv-{i + 1}"
        add_node(node, "namesrv", table({
            "rocketmqHome": home(node), "listenPort": port(9876 + i * 10),
            "bindAddress": "127.0.0.1", "allowInsecurePublicListener": False,
            "kvConfigPath": f"{home(node)}/data/kvConfig.json",
            "configStorePath": f"{home(node)}/data/namesrv.properties",
            "authenticationEnabled": False, "authorizationEnabled": False,
            "enableControllerInNamesrv": False,
        }), {"remoting": endpoint(9876 + i * 10)})

    controller_addresses = ";".join(endpoint(9878 + i * 10) for i in range(3)) if controller else ""
    if controller:
        for i in range(3):
            node = f"controller-{i + 1}"
            content = table({
                "rocketmqHome": home(node), "controllerType": "Raft", "nodeId": i + 1,
                "listenAddr": endpoint(9878 + i * 10), "raftListenAddr": endpoint(9879 + i * 10),
                "configStorePath": f"{home(node)}/data/controller.properties",
                "controllerStorePath": f"{home(node)}/data/controller",
                "storagePath": f"{home(node)}/data/raft",
                "storageBackend": "RocksDB",
                "electionTimeoutMs": 1000, "heartbeatIntervalMs": 300,
                "enableElectUncleanMaster": False, "enableElectUncleanMasterLocal": False,
            })
            for field, base in (("raftPeers", 9879), ("controllerPeers", 9878)):
                for peer in range(3):
                    content += f"\n[[{field}]]\n" + table({"id": peer + 1, "addr": endpoint(base + peer * 10)})
            content += '\n[observability.metrics]\nexporter = "disable"\n'
            add_node(node, "controller", content,
                     {"remoting": endpoint(9878 + i * 10), "raft": endpoint(9879 + i * 10)},
                     {"ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER": "true" if i == 0 else "false"})

    broker_count = 0 if local else (3 if controller else (4 if profile == "dev-ha-2m2s" else (2 if ordinary_ha else 1)))
    for i in range(broker_count):
        node = f"broker-{i + 1}"
        group = i // 2 if ordinary_ha else 0
        replica_id = i + 1 if controller else (i % 2 if ordinary_ha else 0)
        sync = controller or (ordinary_ha and replication != "async")
        role = "SLAVE" if replica_id else ("SYNC_MASTER" if sync else "ASYNC_MASTER")
        store = f"{home(node)}/store"
        broker = {
            "listenPort": port(10911 + i * 20), "brokerIp1": "127.0.0.1", "brokerIp2": "127.0.0.1",
            "storePathRootDir": store, "namesrvAddr": namesrv_addr,
            "autoCreateTopicEnable": True, "enableControllerMode": controller,
        }
        if controller:
            broker["controllerAddr"] = controller_addresses
        content = "[broker]\n" + table(broker)
        content += "\n[broker.brokerIdentity]\n" + table({
            "brokerName": f"broker-{chr(97 + group)}", "brokerClusterName": cluster_name, "brokerId": replica_id,
        })
        content += '\n[broker.brokerServerConfig]\nbindAddress = "127.0.0.1"\n'
        store_config = {
            "storePathRootDir": store, "storePathBrokerIdentity": f"{store}/brokerIdentity",
            "brokerRole": role, "flushDiskType": "SYNC_FLUSH" if sync else "ASYNC_FLUSH",
            "haListenAddress": "127.0.0.1", "haListenPort": port(10912 + i * 20),
            "totalReplicas": 3 if controller else (2 if ordinary_ha else 1),
            "inSyncReplicas": 2 if sync else 1, "minInSyncReplicas": 2 if sync else 1,
            "mappedFileSizeCommitLog": 64 * 1024 * 1024,
        }
        if ordinary_ha and replica_id:
            store_config["haMasterAddress"] = endpoint(10912 + (i - 1) * 20)
        content += "\n[store]\n" + table(store_config)
        add_node(node, "broker", content, {
            "remoting": endpoint(10911 + i * 20), "fast": endpoint(10909 + i * 20),
            "ha": endpoint(10912 + i * 20),
        })

    if proxy:
        content = table({"mode": "local" if local else "cluster"})
        content += "\n[grpc]\n" + table({"listenAddr": endpoint(8081)})
        content += '\n[grpc.tls]\nenabled = false\n'
        content += "\n[remoting]\n" + table({"enabled": proxy_remoting, "listenAddr": endpoint(8080)})
        bindings = {"grpc": endpoint(8081)}
        if proxy_remoting:
            bindings["remoting"] = endpoint(8080)
        if local:
            content += "\n[local]\n" + table({
                "brokerClusterName": cluster_name, "brokerName": "broker-local",
                "brokerIp": "127.0.0.1", "brokerListenPort": port(10911),
                "storeRootDir": f"{home('proxy-1')}/store",
            })
            bindings["embedded-broker-reserved"] = endpoint(10911)
        else:
            content += "\n[cluster]\n" + table({"namesrvAddr": namesrv_addr, "brokerClusterName": cluster_name})
        add_node("proxy-1", "proxy", content, bindings)

    addresses = [binding["address"] for binding in ports]
    if len(addresses) != len(set(addresses)):
        raise ValueError("generated ports collide")
    for service in ("namesrv", "broker", "controller", "proxy"):
        if not any(node["service"] == service for node in nodes):
            continue
        command = ["cargo", "build", "-p", f"rocketmq-{service}", "--bin", f"rocketmq-{service}-rust"]
        if service == "proxy":
            command += ["--no-default-features", "--features", "local-mode" if local else "cluster-mode"]
        builds.append(command)
    builds.append(["cargo", "build", "-p", "rocketmq-admin-cli", "--bin", "rocketmq-admin-cli"])
    groups = [[node["id"] for node in nodes if node["service"] == service]
              for service in ("namesrv", "controller", "broker", "proxy")]
    manifest = {
        "schema_version": 1, "name": name, "profile": profile, "repo_root": repo.as_posix(),
        "output_root": output.as_posix(), "cluster_name": cluster_name, "port_offset": offset,
        "namesrv_addr": namesrv_addr, "controller_addr": controller_addresses,
        "proxy_grpc_addr": endpoint(8081) if proxy else None,
        "replication": (replication or "sync") if ordinary_ha else ("controller" if controller else "none"),
        "build_commands": builds, "start_groups": [group for group in groups if group],
        "nodes": nodes, "ports": ports,
        "notes": ["Generated configuration is not runtime validation.",
                  "Start every member in a dependency group before waiting for group readiness.",
                  "Keep data and verify process identity before stopping or restarting nodes."],
    }
    return manifest, files


def check_ports(manifest):
    with ExitStack() as stack:
        for binding in manifest["ports"]:
            host, port = binding["address"].rsplit(":", 1)
            sock = stack.enter_context(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            try:
                sock.bind((host, int(port)))
            except OSError as error:
                raise ValueError(f"unavailable {binding['node']} {binding['purpose']} {binding['address']}: {error}") from error


def write_plan(manifest, files):
    output = Path(manifest["output_root"])
    # Exclusive creation also rejects existing files, symlinks and reused environment roots.
    output.mkdir(parents=True, exist_ok=False)
    for directory in ("conf", "logs", "run"):
        (output / directory).mkdir()
    for node in manifest["nodes"]:
        for directory in ("data", "store"):
            (Path(node["cwd"]) / directory).mkdir(parents=True, exist_ok=True)
    for relative, content in files.items():
        with (output / relative).open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
    with (output / "manifest.json").open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--profile", choices=PROFILES, default="dev-single")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, help="new directory; defaults to .rocketmq/clusters/NAME")
    parser.add_argument("--port-offset", type=int, default=0)
    parser.add_argument("--with-proxy", action="store_true")
    parser.add_argument("--proxy-remoting", action="store_true")
    parser.add_argument("--replication", choices=("sync", "async"))
    parser.add_argument("--check-ports", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        repo = args.repo_root.resolve()
        for required in ("Cargo.toml", "rocketmq-broker/Cargo.toml", "rocketmq-namesrv/Cargo.toml"):
            if not (repo / required).is_file():
                raise ValueError(f"not a rocketmq-rust checkout: missing {required}")
        output = args.output or repo / ".rocketmq" / "clusters" / args.name
        manifest, files = build_plan(repo, output, args.name, args.profile, args.port_offset,
                                     args.with_proxy, args.replication, args.proxy_remoting)
        if args.check_ports:
            check_ports(manifest)
        if args.dry_run:
            print(json.dumps(manifest, ensure_ascii=False, indent=2))
        else:
            write_plan(manifest, files)
            print(f"Prepared {len(manifest['nodes'])} processes: {manifest['output_root']}/manifest.json")
            print("No processes started. Follow the skill's operations and verification references.")
    except (OSError, ValueError) as error:
        print(f"prepare-cluster: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
