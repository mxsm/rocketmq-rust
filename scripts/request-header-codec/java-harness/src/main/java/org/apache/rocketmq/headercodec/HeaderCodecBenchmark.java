/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.rocketmq.headercodec;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.Reader;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/** Measures the pinned Java production request-header entrypoints. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class HeaderCodecBenchmark {
    private static final Gson GSON = new Gson();

    @Param({
        "clean-controller-canonical-json",
        "clean-controller-canonical-rocketmq",
        "consume-direct-collision-json",
        "consume-direct-collision-rocketmq",
        "consumer-status-inherited-json",
        "consumer-status-inherited-rocketmq",
        "delete-topic-inherited-json",
        "delete-topic-inherited-rocketmq",
        "get-lite-explicit-default-json",
        "get-lite-explicit-default-rocketmq",
        "notification-defaults-json",
        "notification-defaults-rocketmq",
        "pull-fast-inherited-json",
        "pull-fast-inherited-rocketmq",
        "pull-response-fast-json",
        "pull-response-fast-rocketmq",
        "query-consume-queue-sparse-json",
        "query-consume-queue-sparse-rocketmq",
        "send-response-fast-json",
        "send-response-fast-rocketmq",
        "send-v1-fast-json",
        "send-v1-fast-rocketmq",
        "send-v2-fast-json",
        "send-v2-fast-rocketmq"
    })
    public String fixtureId;

    private Class<? extends CommandCustomHeader> headerClass;
    private JsonObject inputFields;
    private int requestCode;
    private SerializeType serializeType;
    private byte[] frame;
    private int expectedFrameLength;
    private String expectedFrameChecksum;
    private RemotingCommand encodeCommand;
    private ByteBuffer decodeFrame;
    private ByteBuf lastOutput;

    @Setup(Level.Trial)
    public void loadFixture() throws Exception {
        Path fixtureDirectory = requiredPath("header.fixtureDirectory").resolve("golden");
        JsonObject fixture = readObject(fixtureDirectory.resolve(fixtureId + ".json"));
        headerClass = Class.forName(fixture.get("javaClass").getAsString()).asSubclass(CommandCustomHeader.class);
        inputFields = fixture.getAsJsonObject("inputFields");
        requestCode = fixture.get("requestCodeValue").getAsInt();
        serializeType = SerializeType.valueOf(fixture.get("serializeType").getAsString());
        frame = java.util.Base64.getDecoder().decode(fixture.get("frameBase64").getAsString());
        expectedFrameLength = fixture.get("frameLength").getAsInt();
        expectedFrameChecksum = fixture.get("fnv1a64").getAsString();
        verifyFreshFrame();
    }

    @Setup(Level.Invocation)
    public void prepareInvocation() throws Exception {
        releaseOutput();
        encodeCommand = createCommand();
        decodeFrame = ByteBuffer.wrap(Arrays.copyOfRange(frame, 4, frame.length));
    }

    @Benchmark
    public void encodeProductionHeader(Blackhole blackhole) {
        ByteBuf output = Unpooled.buffer();
        encodeCommand.fastEncodeHeader(output);
        lastOutput = output;
        blackhole.consume(output);
        blackhole.consume(output.readableBytes());
    }

    @Benchmark
    public void decodeProductionHeader(Blackhole blackhole) throws Exception {
        RemotingCommand command = RemotingCommand.decode(decodeFrame);
        CommandCustomHeader header = command.decodeCommandCustomHeader(headerClass);
        blackhole.consume(command);
        blackhole.consume(header);
    }

    @TearDown(Level.Invocation)
    public void releaseInvocation() {
        releaseOutput();
        encodeCommand = null;
        decodeFrame = null;
    }

    @TearDown(Level.Trial)
    public void verifyTrial() throws Exception {
        verifyFreshFrame();
    }

    private RemotingCommand createCommand() throws Exception {
        CommandCustomHeader header = headerClass.getDeclaredConstructor().newInstance();
        for (Map.Entry<String, JsonElement> entry : inputFields.entrySet()) {
            Field field = findField(headerClass, entry.getKey());
            field.setAccessible(true);
            field.set(header, coerce(entry.getValue(), field.getType()));
        }
        RemotingCommand command = RemotingCommand.createRequestCommand(requestCode, header);
        command.setLanguage(LanguageCode.RUST);
        command.setVersion(501);
        command.setOpaque(7);
        command.setSerializeTypeCurrentRPC(serializeType);
        return command;
    }

    private void verifyFreshFrame() throws Exception {
        ByteBuf output = Unpooled.buffer();
        try {
            createCommand().fastEncodeHeader(output);
            byte[] actual = new byte[output.readableBytes()];
            output.getBytes(output.readerIndex(), actual);
            if (actual.length != expectedFrameLength || !FrameChecksum.fnv1a64(actual).equals(expectedFrameChecksum)) {
                throw new IllegalStateException(fixtureId + " fresh-command frame checksum differs from the fixture");
            }
        } finally {
            output.release();
        }
    }

    private void releaseOutput() {
        if (lastOutput != null) {
            lastOutput.release();
            lastOutput = null;
        }
    }

    private static Field findField(Class<?> type, String name) throws NoSuchFieldException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(type.getName() + "." + name);
    }

    private static Object coerce(JsonElement value, Class<?> target) {
        if (target == String.class) {
            return value.getAsString();
        }
        if (target == Integer.class || target == Integer.TYPE) {
            return value.getAsInt();
        }
        if (target == Long.class || target == Long.TYPE) {
            return value.getAsLong();
        }
        if (target == Boolean.class || target == Boolean.TYPE) {
            return value.getAsBoolean();
        }
        throw new IllegalArgumentException("unsupported fixture field type: " + target.getName());
    }

    private static JsonObject readObject(Path path) throws Exception {
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return GSON.fromJson(reader, JsonObject.class);
        }
    }

    private static Path requiredPath(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("missing -D" + name);
        }
        return Paths.get(value).toAbsolutePath().normalize();
    }
}
