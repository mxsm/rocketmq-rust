/*
 * Copyright 2026 The RocketMQ Rust Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;

/** Actual Java Netty requests against the Rust single-voter contract harness. */
public final class ControllerMetadataCompatibilitySmoke {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected one Controller endpoint");
        }
        NettyClientConfig config = new NettyClientConfig();
        config.setClientWorkerThreads(1);
        config.setClientCallbackExecutorThreads(1);
        NettyRemotingClient client = new NettyRemotingClient(config);
        client.start();
        try {
            for (SerializeType format : SerializeType.values()) {
                RemotingCommand request = request(format);
                RemotingCommand response = client.invokeSync(args[0], request, 5000);
                require(response.getCode() == ResponseCode.SUCCESS, "metadata status");
                require(response.getOpaque() == request.getOpaque(), "request correlation");
                require(response.getBody() == null || response.getBody().length == 0, "legacy empty body");
                GetMetaDataResponseHeader header = response.decodeCommandCustomHeader(GetMetaDataResponseHeader.class);
                require("1".equals(header.getControllerLeaderId()), "leader identity");
                require(args[0].equals(header.getControllerLeaderAddress()), "leader address");
                require(header.isLeader(), "leader flag");
                require(header.getPeers() != null && header.getPeers().contains(args[0]), "peer addresses");

                for (String target : new String[] {"invalid", "1"}) {
                    RemotingCommand probe = request(format);
                    probe.addExtField("checkQuorumForNode", target);
                    RemotingCommand rejected = client.invokeSync(args[0], probe, 5000);
                    require(rejected.getCode() != ResponseCode.SUCCESS, "unsafe rollout rejected");
                    require(rejected.getOpaque() == probe.getOpaque(), "rejection correlation");
                }
                System.out.println("PASS Controller metadata and rollout rejection: " + format);
            }
        } finally {
            client.shutdown();
        }
    }

    private static RemotingCommand request(SerializeType format) {
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_METADATA_INFO, null);
        command.setSerializeTypeCurrentRPC(format);
        return command;
    }

    private static void require(boolean condition, String contract) {
        if (!condition) {
            throw new AssertionError(contract);
        }
    }
}
