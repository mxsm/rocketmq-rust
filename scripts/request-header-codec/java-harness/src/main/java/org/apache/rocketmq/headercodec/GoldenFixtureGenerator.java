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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.SerializeType;

/** Generates deterministic Java production-entrypoint request-header frames. */
public final class GoldenFixtureGenerator {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

    private GoldenFixtureGenerator() {
    }

    public static void main(String[] args) throws Exception {
        Path mappingPath = requiredPath("header.mapping");
        Path inputsPath = requiredPath("header.goldenInputs");
        Path outputDirectory = requiredPath("header.goldenOutput");
        String javaCommit = requiredProperty("header.javaCommit");

        JsonObject mapping = readObject(mappingPath);
        JsonObject inputs = readObject(inputsPath);
        if (!javaCommit.equals(mapping.get("javaCommit").getAsString())
            || !javaCommit.equals(inputs.get("javaCommit").getAsString())) {
            throw new IllegalArgumentException("mapping, inputs, and requested Java commits differ");
        }

        Map<String, String> javaClasses = new HashMap<String, String>();
        for (JsonElement entryElement : mapping.getAsJsonArray("entries")) {
            JsonObject entry = entryElement.getAsJsonObject();
            if (!entry.get("javaClass").isJsonNull()) {
                javaClasses.put(entry.get("rustTypeId").getAsString(), entry.get("javaClass").getAsString());
            }
        }

        Files.createDirectories(outputDirectory);
        List<JsonObject> generated = new ArrayList<JsonObject>();
        for (JsonElement caseElement : inputs.getAsJsonArray("cases")) {
            JsonObject fixtureCase = caseElement.getAsJsonObject();
            String rustTypeId = fixtureCase.get("rustTypeId").getAsString();
            String javaClass = javaClasses.get(rustTypeId);
            if (javaClass == null) {
                throw new IllegalArgumentException("unmapped fixture type: " + rustTypeId);
            }
            for (JsonElement typeElement : fixtureCase.getAsJsonArray("serializeTypes")) {
                SerializeType serializeType = SerializeType.valueOf(typeElement.getAsString());
                generated.add(generateFixture(fixtureCase, javaClass, serializeType, outputDirectory, javaCommit));
            }
        }
        Collections.sort(generated, Comparator.comparing(value -> value.get("id").getAsString()));

        JsonObject index = new JsonObject();
        index.addProperty("schemaVersion", 1);
        index.addProperty("javaCommit", javaCommit);
        index.addProperty("fixtureCount", generated.size());
        com.google.gson.JsonArray fixtures = new com.google.gson.JsonArray();
        for (JsonObject fixture : generated) {
            fixtures.add(fixture);
        }
        index.add("fixtures", fixtures);
        writeObject(outputDirectory.resolve("index.json"), index);
        System.out.println("wrote " + generated.size() + " Java request-header golden fixtures");
    }

    private static JsonObject generateFixture(JsonObject fixtureCase, String javaClass, SerializeType serializeType,
        Path outputDirectory, String javaCommit) throws Exception {
        Class<?> headerClass = Class.forName(javaClass);
        Object instance = headerClass.getDeclaredConstructor().newInstance();
        JsonObject fields = fixtureCase.getAsJsonObject("fields");
        for (Map.Entry<String, JsonElement> entry : fields.entrySet()) {
            Field field = findField(headerClass, entry.getKey());
            field.setAccessible(true);
            field.set(instance, coerce(entry.getValue(), field.getType()));
        }

        int requestCode = RequestCode.class.getField(fixtureCase.get("requestCode").getAsString()).getInt(null);
        RemotingCommand command = RemotingCommand.createRequestCommand(requestCode, (CommandCustomHeader) instance);
        command.setLanguage(LanguageCode.RUST);
        command.setVersion(501);
        command.setOpaque(7);
        command.setSerializeTypeCurrentRPC(serializeType);

        ByteBuf output = Unpooled.buffer();
        command.fastEncodeHeader(output);
        byte[] frame = new byte[output.readableBytes()];
        output.getBytes(output.readerIndex(), frame);

        JsonObject fixture = new JsonObject();
        String id = fixtureCase.get("id").getAsString() + "-" + serializeType.name().toLowerCase();
        fixture.addProperty("schemaVersion", 1);
        fixture.addProperty("id", id);
        fixture.addProperty("javaCommit", javaCommit);
        fixture.addProperty("rustTypeId", fixtureCase.get("rustTypeId").getAsString());
        fixture.addProperty("javaClass", javaClass);
        fixture.addProperty("requestCode", fixtureCase.get("requestCode").getAsString());
        fixture.addProperty("requestCodeValue", requestCode);
        fixture.addProperty("wireCodeValue", serializeType == SerializeType.ROCKETMQ ? (short) requestCode : requestCode);
        fixture.addProperty("direction", fixtureCase.get("direction").getAsString());
        fixture.addProperty("serializeType", serializeType.name());
        fixture.addProperty("actualPath", serializeType == SerializeType.ROCKETMQ
            && org.apache.rocketmq.remoting.protocol.FastCodesHeader.class.isAssignableFrom(headerClass)
            ? "fast" : "normal");
        fixture.add("inputFields", fields.deepCopy());
        fixture.add("declaredLogicalFields", mapToJson(logicalFields(headerClass, instance)));
        RemotingCommand decoded = RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length));
        Map<String, String> actualFields = decoded.getExtFields() == null
            ? Collections.<String, String>emptyMap() : new TreeMap<String, String>(decoded.getExtFields());
        fixture.add("canonicalExtFields", mapToJson(actualFields));
        fixture.addProperty("frameBase64", Base64.getEncoder().encodeToString(frame));
        fixture.addProperty("frameLength", frame.length);
        fixture.addProperty("fnv1a64", FrameChecksum.fnv1a64(frame));
        fixture.addProperty("opaque", 7);
        fixture.addProperty("version", 501);
        fixture.addProperty("language", "RUST");

        Path fixtureOutput = outputDirectory.resolve(id + ".json");
        writeObject(fixtureOutput, fixture);

        JsonObject indexEntry = new JsonObject();
        indexEntry.addProperty("id", id);
        indexEntry.addProperty("file", id + ".json");
        indexEntry.addProperty("rustTypeId", fixtureCase.get("rustTypeId").getAsString());
        indexEntry.addProperty("serializeType", serializeType.name());
        indexEntry.addProperty("frameLength", frame.length);
        indexEntry.addProperty("fnv1a64", FrameChecksum.fnv1a64(frame));
        return indexEntry;
    }

    private static TreeMap<String, String> logicalFields(Class<?> headerClass, Object instance)
        throws IllegalAccessException {
        TreeMap<String, String> result = new TreeMap<String, String>();
        Class<?> current = headerClass;
        while (current != null && CommandCustomHeader.class.isAssignableFrom(current)) {
            for (Field field : current.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                    continue;
                }
                field.setAccessible(true);
                Object value = field.get(instance);
                if (value != null) {
                    result.put(field.getName(), value.toString());
                }
            }
            current = current.getSuperclass();
        }
        return result;
    }

    private static JsonObject mapToJson(Map<String, String> values) {
        JsonObject result = new JsonObject();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            result.addProperty(entry.getKey(), entry.getValue());
        }
        return result;
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

    private static void writeObject(Path path, JsonObject value) throws Exception {
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            GSON.toJson(value, writer);
            writer.write('\n');
        }
    }

    private static Path requiredPath(String name) {
        return Paths.get(requiredProperty(name)).toAbsolutePath().normalize();
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("missing -D" + name);
        }
        return value;
    }
}
