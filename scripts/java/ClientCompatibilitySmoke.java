/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;

public class ClientCompatibilitySmoke {
    private static final List<String> DEFAULT_SCENARIOS = Arrays.asList(
        "ProducerSync",
        "ProducerAsync",
        "ProducerOneway",
        "ProducerBatch",
        "TransactionProducer",
        "PushConcurrent",
        "LitePullAssign",
        "LitePullSubscribe",
        "RequestReply"
    );

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        System.out.printf(
            "RocketMQ Java client compatibility smoke namesrv=%s topic=%s scenarios=%s acl=%s trace=%s tls=%s%n",
            config.namesrvAddr, config.topic, config.scenarios, config.acl, config.trace, config.tls);

        for (String scenario : config.scenarios) {
            runScenario(config, scenario);
            System.out.printf("SCENARIO %s PASS%n", scenario);
        }
    }

    private static void runScenario(Config config, String scenario) throws Exception {
        switch (scenario) {
            case "ProducerSync":
                runProducerSync(config);
                return;
            case "ProducerAsync":
                runProducerAsync(config);
                return;
            case "ProducerOneway":
                runProducerOneway(config);
                return;
            case "ProducerBatch":
                runProducerBatch(config);
                return;
            case "ProducerBatchBenchmark":
                runProducerBatchBenchmark(config);
                return;
            case "ProducerRecall":
                runProducerRecall(config);
                return;
            case "TransactionProducer":
                runTransactionProducer(config);
                return;
            case "PushConcurrent":
                runPushConcurrent(config);
                return;
            case "LitePullAssign":
                runLitePullAssign(config);
                return;
            case "LitePullSubscribe":
                runLitePullSubscribe(config);
                return;
            case "LitePullBenchmark":
                runLitePullBenchmark(config);
                return;
            case "RequestReply":
                runRequestReply(config);
                return;
            default:
                throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }

    private static void runProducerSync(Config config) throws Exception {
        DefaultMQProducer producer = newProducer(config, "java-sync");
        try {
            producer.start();
            for (int i = 0; i < config.messageCount; i++) {
                SendResult result = producer.send(message(config.topic, "JavaSync", "java-sync-" + i), config.timeoutMillis);
                assertSendOk("ProducerSync", result);
            }
        } finally {
            producer.shutdown();
        }
    }

    private static void runProducerAsync(Config config) throws Exception {
        DefaultMQProducer producer = newProducer(config, "java-async");
        CountDownLatch latch = new CountDownLatch(config.messageCount);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            producer.start();
            for (int i = 0; i < config.messageCount; i++) {
                producer.send(message(config.topic, "JavaAsync", "java-async-" + i), new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        try {
                            assertSendOk("ProducerAsync", sendResult);
                        } catch (Throwable error) {
                            failure.compareAndSet(null, error);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onException(Throwable error) {
                        failure.compareAndSet(null, error);
                        latch.countDown();
                    }
                });
            }

            await("ProducerAsync callbacks", latch, config.timeoutMillis);
            if (failure.get() != null) {
                throw new AssertionError("ProducerAsync failed", failure.get());
            }
        } finally {
            producer.shutdown();
        }
    }

    private static void runProducerOneway(Config config) throws Exception {
        DefaultMQProducer producer = newProducer(config, "java-oneway");
        try {
            producer.start();
            for (int i = 0; i < config.messageCount; i++) {
                producer.sendOneway(message(config.topic, "JavaOneway", "java-oneway-" + i));
            }
        } finally {
            producer.shutdown();
        }
    }

    private static void runProducerBatch(Config config) throws Exception {
        DefaultMQProducer producer = newProducer(config, "java-batch");
        try {
            producer.start();
            List<Message> messages = new ArrayList<>();
            for (int i = 0; i < Math.max(3, config.messageCount); i++) {
                messages.add(message(config.topic, "JavaBatch", "java-batch-" + i));
            }
            assertSendOk("ProducerBatch", producer.send(messages, config.timeoutMillis));
        } finally {
            producer.shutdown();
        }
    }

    private static void runProducerBatchBenchmark(Config config) throws Exception {
        byte[] body = fixedBody(config.messageSize);
        DefaultMQProducer producer = newProducer(config, "java-batch-bench");
        try {
            producer.start();
            int remaining = config.messageCount;
            int successCount = 0;
            int failedCount = 0;
            int responseFailedCount = 0;
            long totalRtMillis = 0L;
            long maxRtMillis = 0L;
            long startNanos = System.nanoTime();

            while (remaining > 0) {
                int batchSize = Math.min(remaining, config.batchSize);
                List<Message> messages = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    messages.add(message(config.topic, "JavaBatchBenchmark", body));
                }

                long beginNanos = System.nanoTime();
                try {
                    SendResult result = producer.send(messages, config.timeoutMillis);
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beginNanos);
                    if (result != null && result.getSendStatus() == SendStatus.SEND_OK) {
                        successCount += batchSize;
                    } else {
                        responseFailedCount += batchSize;
                    }
                    totalRtMillis += elapsedMillis * batchSize;
                    maxRtMillis = Math.max(maxRtMillis, elapsedMillis);
                } catch (Throwable error) {
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beginNanos);
                    failedCount += batchSize;
                    totalRtMillis += elapsedMillis * batchSize;
                    maxRtMillis = Math.max(maxRtMillis, elapsedMillis);
                    System.err.println("ProducerBatchBenchmark send failed: " + error);
                }

                remaining -= batchSize;
            }

            long elapsedNanos = System.nanoTime() - startNanos;
            printCompleteSummary(
                "Send",
                successCount,
                failedCount,
                responseFailedCount,
                totalRtMillis,
                maxRtMillis,
                elapsedNanos);
        } finally {
            producer.shutdown();
        }
    }

    private static void runProducerRecall(Config config) throws Exception {
        DefaultMQProducer producer = newProducer(config, "java-recall");
        try {
            producer.start();
            Message delayed = message(config.topic, "JavaRecall", "java-recall");
            delayed.setDelayTimeMs(60_000L);
            SendResult sendResult = producer.send(delayed, config.timeoutMillis);
            assertSendOk("ProducerRecall send", sendResult);
            String recallHandle = sendResult.getRecallHandle();
            if (recallHandle == null || recallHandle.isEmpty()) {
                throw new AssertionError("ProducerRecall broker did not return a recall handle");
            }
            String recalledMsgId = producer.recallMessage(config.topic, recallHandle);
            if (recalledMsgId == null || recalledMsgId.isEmpty()) {
                throw new AssertionError("ProducerRecall returned an empty recalled message id");
            }
        } finally {
            producer.shutdown();
        }
    }

    private static void runTransactionProducer(Config config) throws Exception {
        TransactionMQProducer producer =
            new TransactionMQProducer(uniqueGroup("java-transaction"), rpcHook(config), config.trace, null);
        configureProducer(config, producer);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        try {
            producer.start();
            TransactionSendResult result =
                producer.sendMessageInTransaction(message(config.topic, "JavaTransaction", "java-transaction"), null);
            assertSendOk("TransactionProducer", result);
            if (result.getLocalTransactionState() != LocalTransactionState.COMMIT_MESSAGE) {
                throw new AssertionError("TransactionProducer local state was " + result.getLocalTransactionState());
            }
        } finally {
            producer.shutdown();
        }
    }

    private static void runPushConcurrent(Config config) throws Exception {
        String tag = "JavaPushConcurrent";
        String body = "java-push-" + System.nanoTime();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        DefaultMQPushConsumer consumer = newPushConsumer(config, "java-push");
        DefaultMQProducer producer = newProducer(config, "java-push-producer");

        try {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setConsumeThreadMin(1);
            consumer.setConsumeThreadMax(2);
            consumer.setConsumeMessageBatchMaxSize(1);
            consumer.setPullBatchSize(4);
            consumer.subscribe(config.topic, tag);
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        if (bodyEquals(msg, body)) {
                            latch.countDown();
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
            Thread.sleep(1500L);

            producer.start();
            assertSendOk("PushConcurrent seed", producer.send(message(config.topic, tag, body), config.timeoutMillis));
            await("PushConcurrent delivery", latch, config.timeoutMillis);
            if (failure.get() != null) {
                throw new AssertionError("PushConcurrent failed", failure.get());
            }
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    private static void runLitePullAssign(Config config) throws Exception {
        String tag = "JavaLitePullAssign";
        String body = "java-lite-pull-assign-" + System.nanoTime();
        DefaultMQProducer producer = newProducer(config, "java-lite-assign-producer");
        DefaultLitePullConsumer consumer = newLitePullConsumer(config, "java-lite-assign");

        try {
            producer.start();
            consumer.setAutoCommit(false);
            consumer.setPullBatchSize(4);
            consumer.setPollTimeoutMillis(500L);
            consumer.setSubExpressionForAssign(config.topic, tag);
            consumer.start();

            Collection<MessageQueue> queues = consumer.fetchMessageQueues(config.topic);
            if (queues.isEmpty()) {
                throw new AssertionError("LitePullAssign found no queues for topic " + config.topic);
            }
            MessageQueue queue = queues.iterator().next();
            consumer.assign(Collections.singletonList(queue));
            long offset = Math.max(0L, producer.maxOffset(queue));
            consumer.pause(Collections.singletonList(queue));
            consumer.resume(Collections.singletonList(queue));
            consumer.seek(queue, offset);
            assertSendOk("LitePullAssign seed", producer.send(message(config.topic, tag, body), queue, config.timeoutMillis));
            pollUntilBody(consumer, body, config.timeoutMillis);
            consumer.commit();
            consumer.committed(queue);
            consumer.seekToEnd(queue);
        } finally {
            consumer.shutdown();
            producer.shutdown();
        }
    }

    private static void runLitePullSubscribe(Config config) throws Exception {
        String tag = "JavaLitePullSubscribe";
        String body = "java-lite-pull-subscribe-" + System.nanoTime();
        DefaultLitePullConsumer consumer = newLitePullConsumer(config, "java-lite-subscribe");
        DefaultMQProducer producer = newProducer(config, "java-lite-subscribe-producer");

        try {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setAutoCommit(true);
            consumer.setPullBatchSize(4);
            consumer.setPollTimeoutMillis(500L);
            consumer.subscribe(config.topic, tag);
            consumer.start();
            waitForAssignment(consumer, config.timeoutMillis);

            producer.start();
            assertSendOk("LitePullSubscribe seed", producer.send(message(config.topic, tag, body), config.timeoutMillis));
            pollUntilBody(consumer, body, config.timeoutMillis);
            if (!consumer.isAutoCommit()) {
                throw new AssertionError("LitePullSubscribe expected auto commit to be enabled");
            }
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    private static void runLitePullBenchmark(Config config) throws Exception {
        String tag = "JavaLitePullBenchmark";
        byte[] body = fixedBody(config.messageSize);
        DefaultMQProducer producer = newProducer(config, "java-lite-bench-producer");
        DefaultLitePullConsumer consumer = newLitePullConsumer(config, "java-lite-bench");

        try {
            producer.start();
            consumer.setAutoCommit(false);
            consumer.setPullBatchSize(config.batchSize);
            consumer.setPollTimeoutMillis(Math.min(config.timeoutMillis, 1000));
            consumer.setSubExpressionForAssign(config.topic, tag);
            consumer.start();

            Collection<MessageQueue> queues = consumer.fetchMessageQueues(config.topic);
            if (queues.isEmpty()) {
                throw new AssertionError("LitePullBenchmark found no queues for topic " + config.topic);
            }
            MessageQueue queue = queues.iterator().next();
            consumer.assign(Collections.singletonList(queue));
            long offset = Math.max(0L, producer.maxOffset(queue));
            consumer.seek(queue, offset);

            for (int i = 0; i < config.messageCount; i++) {
                assertSendOk("LitePullBenchmark seed", producer.send(message(config.topic, tag, body), queue, config.timeoutMillis));
            }

            long startNanos = System.nanoTime();
            long deadlineMillis = System.currentTimeMillis() + Math.max(1000L, config.timeoutMillis * 2L);
            int successCount = 0;
            int failedCount = 0;
            long totalRtMillis = 0L;
            long maxRtMillis = 0L;

            while (successCount < config.messageCount && System.currentTimeMillis() < deadlineMillis) {
                long pollStartNanos = System.nanoTime();
                List<MessageExt> messages = consumer.poll(Math.min(config.timeoutMillis, 1000));
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pollStartNanos);
                if (messages.isEmpty()) {
                    continue;
                }

                for (MessageExt message : messages) {
                    if (successCount >= config.messageCount) {
                        break;
                    }
                    if (tag.equals(message.getTags())) {
                        successCount++;
                        totalRtMillis += elapsedMillis;
                        maxRtMillis = Math.max(maxRtMillis, elapsedMillis);
                    }
                }
            }

            if (successCount < config.messageCount) {
                failedCount = config.messageCount - successCount;
            }
            long elapsedNanos = System.nanoTime() - startNanos;
            printCompleteSummary("Consume", successCount, failedCount, 0, totalRtMillis, maxRtMillis, elapsedNanos);
            consumer.commit();
        } finally {
            consumer.shutdown();
            producer.shutdown();
        }
    }

    private static void runRequestReply(Config config) throws Exception {
        String tag = "JavaRequestReply";
        String requestBody = "java-request-" + System.nanoTime();
        String replyBody = "java-reply-" + System.nanoTime();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        DefaultMQProducer replyProducer = newProducer(config, "java-reply-producer");
        DefaultMQPushConsumer responder = newPushConsumer(config, "java-reply-consumer");
        DefaultMQProducer requester = newProducer(config, "java-request-producer");

        try {
            replyProducer.start();
            responder.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            responder.setConsumeThreadMin(1);
            responder.setConsumeThreadMax(2);
            responder.setConsumeMessageBatchMaxSize(1);
            responder.setPullBatchSize(4);
            responder.subscribe(config.topic, tag);
            responder.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        if (!bodyEquals(msg, requestBody)) {
                            continue;
                        }
                        try {
                            Message replyMessage = MessageUtil.createReplyMessage(msg, bytes(replyBody));
                            replyProducer.send(replyMessage, config.timeoutMillis);
                        } catch (Throwable error) {
                            failure.compareAndSet(null, error);
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            responder.start();
            Thread.sleep(1500L);

            requester.start();
            Message response = requester.request(message(config.topic, tag, requestBody), config.timeoutMillis);
            if (failure.get() != null) {
                throw new AssertionError("RequestReply responder failed", failure.get());
            }
            if (response == null || response.getBody() == null || !replyBody.equals(new String(response.getBody(), StandardCharsets.UTF_8))) {
                throw new AssertionError("RequestReply returned unexpected response");
            }
        } finally {
            requester.shutdown();
            responder.shutdown();
            replyProducer.shutdown();
        }
    }

    private static DefaultMQProducer newProducer(Config config, String groupPrefix) {
        DefaultMQProducer producer = new DefaultMQProducer(uniqueGroup(groupPrefix), rpcHook(config), config.trace, null);
        configureProducer(config, producer);
        return producer;
    }

    private static void configureProducer(Config config, DefaultMQProducer producer) {
        producer.setNamesrvAddr(config.namesrvAddr);
        producer.setSendMsgTimeout(config.timeoutMillis);
        producer.setUseTLS(config.tls);
        producer.setInstanceName(Long.toString(System.nanoTime()));
    }

    private static DefaultMQPushConsumer newPushConsumer(Config config, String groupPrefix) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
            uniqueGroup(groupPrefix),
            rpcHook(config),
            new AllocateMessageQueueAveragely(),
            config.trace,
            null);
        consumer.setNamesrvAddr(config.namesrvAddr);
        consumer.setUseTLS(config.tls);
        consumer.setInstanceName(Long.toString(System.nanoTime()));
        return consumer;
    }

    private static DefaultLitePullConsumer newLitePullConsumer(Config config, String groupPrefix) {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(uniqueGroup(groupPrefix), rpcHook(config));
        consumer.setNamesrvAddr(config.namesrvAddr);
        consumer.setUseTLS(config.tls);
        consumer.setInstanceName(Long.toString(System.nanoTime()));
        return consumer;
    }

    private static RPCHook rpcHook(Config config) {
        if (!config.acl) {
            return null;
        }
        SessionCredentials credentials = config.securityToken.isEmpty()
            ? new SessionCredentials(config.accessKey, config.secretKey)
            : new SessionCredentials(config.accessKey, config.secretKey, config.securityToken);
        return new AclClientRPCHook(credentials);
    }

    private static Message message(String topic, String tag, String body) {
        return new Message(topic, tag, bytes(body));
    }

    private static Message message(String topic, String tag, byte[] body) {
        return new Message(topic, tag, body);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] fixedBody(int size) {
        byte[] body = new byte[size];
        Arrays.fill(body, (byte)'a');
        return body;
    }

    private static boolean bodyEquals(MessageExt message, String expectedBody) {
        return message.getBody() != null && expectedBody.equals(new String(message.getBody(), StandardCharsets.UTF_8));
    }

    private static void assertSendOk(String scenario, SendResult result) {
        if (result == null) {
            throw new AssertionError(scenario + " returned null SendResult");
        }
        if (result.getSendStatus() != SendStatus.SEND_OK) {
            throw new AssertionError(scenario + " send status was " + result.getSendStatus());
        }
        if (result.getMsgId() == null || result.getMsgId().isEmpty()) {
            throw new AssertionError(scenario + " returned an empty message id");
        }
    }

    private static void await(String operation, CountDownLatch latch, int timeoutMillis) throws InterruptedException {
        if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new AssertionError(operation + " timed out after " + timeoutMillis + "ms");
        }
    }

    private static void pollUntilBody(DefaultLitePullConsumer consumer, String expectedBody, int timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<MessageExt> messages = consumer.poll(1000L);
            for (MessageExt message : messages) {
                if (bodyEquals(message, expectedBody)) {
                    return;
                }
            }
        }
        throw new AssertionError("LitePull did not receive expected body before timeout");
    }

    private static void waitForAssignment(DefaultLitePullConsumer consumer, int timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            Set<MessageQueue> assignment = consumer.assignment();
            if (!assignment.isEmpty()) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("LitePullSubscribe assignment timed out");
    }

    private static void printCompleteSummary(
        String operation,
        int successCount,
        int failedCount,
        int responseFailedCount,
        long totalRtMillis,
        long maxRtMillis,
        long elapsedNanos
    ) {
        double elapsedSeconds = Math.max(0.001D, elapsedNanos / 1_000_000_000D);
        long tps = Math.round(successCount / elapsedSeconds);
        double averageRtMillis = successCount == 0 ? 0.0D : (double)totalRtMillis / successCount;
        System.out.printf(
            "[Complete] %s Total: %d | %s TPS: %d | Max RT(ms): %d | Average RT(ms): %.3f | %s Failed: %d | Response Failed: %d%n",
            operation,
            successCount + failedCount,
            operation,
            tps,
            maxRtMillis,
            averageRtMillis,
            operation,
            failedCount,
            responseFailedCount);
    }

    private static String uniqueGroup(String prefix) {
        return "rocketmq-java-smoke-" + prefix + "-" + System.currentTimeMillis() + "-" + System.nanoTime();
    }

    private static final class Config {
        private String namesrvAddr = "";
        private String topic = "TopicTest";
        private List<String> scenarios = DEFAULT_SCENARIOS;
        private int messageCount = 1;
        private int messageSize = 128;
        private int batchSize = 16;
        private int timeoutMillis = 30_000;
        private boolean acl;
        private boolean trace;
        private boolean tls;
        private String accessKey = "";
        private String secretKey = "";
        private String securityToken = "";

        static Config parse(String[] args) {
            Config config = new Config();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--namesrv":
                        config.namesrvAddr = requireValue(args, ++i, arg);
                        break;
                    case "--topic":
                        config.topic = requireValue(args, ++i, arg);
                        break;
                    case "--scenario":
                    case "--scenarios":
                        config.scenarios = parseScenarios(requireValue(args, ++i, arg));
                        break;
                    case "--message-count":
                        config.messageCount = Integer.parseInt(requireValue(args, ++i, arg));
                        break;
                    case "--message-size":
                        config.messageSize = Integer.parseInt(requireValue(args, ++i, arg));
                        break;
                    case "--batch-size":
                        config.batchSize = Integer.parseInt(requireValue(args, ++i, arg));
                        break;
                    case "--timeout-ms":
                        config.timeoutMillis = Integer.parseInt(requireValue(args, ++i, arg));
                        break;
                    case "--acl":
                        config.acl = true;
                        break;
                    case "--trace":
                        config.trace = true;
                        break;
                    case "--tls":
                        config.tls = true;
                        break;
                    case "--access-key":
                        config.accessKey = requireValue(args, ++i, arg);
                        break;
                    case "--secret-key":
                        config.secretKey = requireValue(args, ++i, arg);
                        break;
                    case "--security-token":
                        config.securityToken = requireValue(args, ++i, arg);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }

            if (isBlank(config.namesrvAddr)) {
                throw new IllegalArgumentException("--namesrv is required");
            }
            if (isBlank(config.topic)) {
                throw new IllegalArgumentException("--topic is required");
            }
            if (config.messageCount <= 0) {
                throw new IllegalArgumentException("--message-count must be positive");
            }
            if (config.messageSize <= 0) {
                throw new IllegalArgumentException("--message-size must be positive");
            }
            if (config.batchSize <= 0) {
                throw new IllegalArgumentException("--batch-size must be positive");
            }
            if (config.timeoutMillis <= 0) {
                throw new IllegalArgumentException("--timeout-ms must be positive");
            }
            if (config.acl && (isBlank(config.accessKey) || isBlank(config.secretKey))) {
                throw new IllegalArgumentException("--acl requires --access-key and --secret-key");
            }
            return config;
        }

        private static List<String> parseScenarios(String value) {
            Set<String> scenarios = new LinkedHashSet<>();
            for (String item : value.split(",")) {
                String scenario = item.trim();
                if (!scenario.isEmpty()) {
                    scenarios.add(scenario);
                }
            }
            if (scenarios.isEmpty()) {
                throw new IllegalArgumentException("--scenarios must contain at least one scenario");
            }
            return new ArrayList<>(scenarios);
        }

        private static String requireValue(String[] args, int index, String option) {
            if (index >= args.length || args[index].startsWith("--")) {
                throw new IllegalArgumentException(option + " requires a value");
            }
            return args[index];
        }

        private static boolean isBlank(String value) {
            return value == null || value.trim().isEmpty();
        }
    }
}
