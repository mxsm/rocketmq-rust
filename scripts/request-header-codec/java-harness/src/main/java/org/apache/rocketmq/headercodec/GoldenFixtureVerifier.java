/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.rocketmq.headercodec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/** Verifies Rust-produced production frames with the pinned Java decoder. */
public final class GoldenFixtureVerifier {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private GoldenFixtureVerifier() {
    }

    public static void main(String[] args) throws Exception {
        Path mappingPath = requiredPath("header.mapping");
        Path fixtureDirectory = requiredPath("header.fixtureDirectory");
        Path rustFrameDirectory = requiredPath("header.rustFrameDirectory");

        Map<String, String> javaClasses = new HashMap<String, String>();
        JsonObject mapping = readObject(mappingPath);
        for (JsonElement entryElement : mapping.getAsJsonArray("entries")) {
            JsonObject entry = entryElement.getAsJsonObject();
            if (!entry.get("javaClass").isJsonNull()) {
                javaClasses.put(entry.get("rustTypeId").getAsString(), entry.get("javaClass").getAsString());
            }
        }

        JsonObject index = readObject(fixtureDirectory.resolve("golden/index.json"));
        int verified = 0;
        for (JsonElement entryElement : index.getAsJsonArray("fixtures")) {
            JsonObject entry = entryElement.getAsJsonObject();
            JsonObject fixture = readObject(fixtureDirectory.resolve("golden").resolve(entry.get("file").getAsString()));
            String id = fixture.get("id").getAsString();
            byte[] frame = Files.readAllBytes(rustFrameDirectory.resolve(id + ".bin"));
            if (frame.length < 8) {
                throw new IllegalArgumentException(id + " Rust frame is too short");
            }

            RemotingCommand command = RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length));
            Map<String, String> expected = jsonMap(fixture.getAsJsonObject("canonicalExtFields"));
            Map<String, String> actual = command.getExtFields();
            if (actual == null || !expected.equals(actual)) {
                throw new IllegalStateException(id + " extension fields differ: expected=" + expected + ", actual=" + actual);
            }

            String className = javaClasses.get(fixture.get("rustTypeId").getAsString());
            if (className == null) {
                throw new IllegalArgumentException(id + " has no Java class mapping");
            }
            Class<?> rawClass = Class.forName(className);
            if (!CommandCustomHeader.class.isAssignableFrom(rawClass)) {
                throw new IllegalArgumentException(className + " is not a CommandCustomHeader");
            }
            @SuppressWarnings("unchecked")
            Class<? extends CommandCustomHeader> headerClass =
                (Class<? extends CommandCustomHeader>) rawClass;
            CommandCustomHeader header = command.decodeCommandCustomHeader(headerClass);
            if (header == null) {
                throw new IllegalStateException(id + " did not produce a typed Java header");
            }
            verified++;
        }
        System.out.println("verified " + verified + " Rust request-header frames with the Java production decoder");
    }

    private static Map<String, String> jsonMap(JsonObject object) {
        Map<String, String> result = new HashMap<String, String>();
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getAsString());
        }
        return result;
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
