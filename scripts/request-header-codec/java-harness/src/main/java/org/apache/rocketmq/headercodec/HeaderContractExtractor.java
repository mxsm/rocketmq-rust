/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.rocketmq.headercodec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;

/** Extracts a deterministic request-header schema from a pinned Java build. */
public final class HeaderContractExtractor {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

    private HeaderContractExtractor() {
    }

    public static void main(String[] args) throws Exception {
        Path mappingPath = requiredPath("header.mapping");
        Path overridesPath = requiredPath("header.overrides");
        Path outputPath = requiredPath("header.output");
        String javaCommit = requiredProperty("header.javaCommit");

        JsonObject mapping = readObject(mappingPath);
        if (!javaCommit.equals(mapping.get("javaCommit").getAsString())) {
            throw new IllegalArgumentException("mapping and requested Java commits differ");
        }
        Map<String, String> defaultOverrides = loadDefaultOverrides(overridesPath);

        List<JsonObject> headers = new ArrayList<JsonObject>();
        for (JsonElement element : mapping.getAsJsonArray("entries")) {
            JsonObject entry = element.getAsJsonObject();
            if (entry.get("javaClass").isJsonNull()) {
                continue;
            }
            headers.add(extractHeader(entry, defaultOverrides));
        }
        Collections.sort(headers, Comparator.comparing(value -> value.get("rustTypeId").getAsString()));

        JsonObject schema = new JsonObject();
        schema.addProperty("schemaVersion", 1);
        schema.addProperty("javaCommit", javaCommit);
        schema.addProperty("mappingEntryCount", mapping.get("entryCount").getAsInt());
        schema.addProperty("mappedHeaderCount", headers.size());
        JsonArray headerArray = new JsonArray();
        for (JsonObject header : headers) {
            headerArray.add(header);
        }
        schema.add("headers", headerArray);
        writeObject(outputPath, schema);
        System.out.println("wrote Java request-header schema with " + headers.size() + " mapped headers");
    }

    private static JsonObject extractHeader(JsonObject mapping, Map<String, String> defaultOverrides)
        throws ClassNotFoundException {
        String rustType = mapping.get("rustType").getAsString();
        String rustTypeId = mapping.get("rustTypeId").getAsString();
        String className = mapping.get("javaClass").getAsString();
        Class<?> type = Class.forName(className);
        Object instance = instantiate(type);

        JsonObject header = new JsonObject();
        header.addProperty("rustTypeId", rustTypeId);
        header.addProperty("rustType", rustType);
        header.addProperty("javaClass", className);
        header.addProperty("javaFast", FastCodesHeader.class.isAssignableFrom(type));
        header.add("requestCodes", mapping.getAsJsonArray("requestCodes").deepCopy());

        JsonArray hierarchy = new JsonArray();
        List<Class<?>> classes = hierarchy(type);
        for (Class<?> current : classes) {
            hierarchy.add(current.getName());
        }
        header.add("classHierarchy", hierarchy);

        List<FieldRecord> records = new ArrayList<FieldRecord>();
        for (int depth = 0; depth < classes.size(); depth++) {
            Class<?> current = classes.get(depth);
            for (Field field : current.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                    continue;
                }
                String overrideKey = rustType + "#" + field.getName();
                records.add(extractField(field, instance, depth, defaultOverrides.get(overrideKey)));
            }
        }
        Collections.sort(records, Comparator
            .comparingInt((FieldRecord record) -> record.inheritanceDepth)
            .thenComparing(record -> record.key));
        JsonArray fields = new JsonArray();
        for (FieldRecord record : records) {
            fields.add(record.toJson());
        }
        header.add("fields", fields);
        return header;
    }

    private static FieldRecord extractField(Field field, Object instance, int depth, String override) {
        boolean notNull = hasAnnotation(field, "CFNotNull");
        boolean nullable = hasAnnotation(field, "CFNullable");
        String presence;
        if (notNull) {
            presence = "required";
        } else if (nullable || !field.getType().isPrimitive()) {
            presence = "optional";
        } else {
            presence = "primitive";
        }

        String defaultSemantic = override;
        if (defaultSemantic == null) {
            defaultSemantic = readDefault(field, instance);
        }
        return new FieldRecord(
            field.getName(),
            field.getType().getName(),
            presence,
            defaultSemantic,
            field.getDeclaringClass().getName(),
            depth
        );
    }

    private static String readDefault(Field field, Object instance) {
        if (instance == null || !field.getDeclaringClass().isInstance(instance)) {
            return "unavailable";
        }
        try {
            field.setAccessible(true);
            Object value = field.get(instance);
            if (value == null) {
                return "null";
            }
            if (value instanceof Enum<?>) {
                return "literal:" + ((Enum<?>) value).name();
            }
            if (value instanceof String || value instanceof Number || value instanceof Boolean
                || value instanceof Character) {
                return "literal:" + String.valueOf(value).toLowerCase(Locale.ROOT);
            }
            return "object:" + value.getClass().getName();
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return "unavailable";
        }
    }

    private static Object instantiate(Class<?> type) {
        if (Modifier.isAbstract(type.getModifiers())) {
            return null;
        }
        try {
            Constructor<?> constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return null;
        }
    }

    private static List<Class<?>> hierarchy(Class<?> type) {
        List<Class<?>> result = new ArrayList<Class<?>>();
        Class<?> current = type;
        while (current != null && current != Object.class) {
            result.add(current);
            current = current.getSuperclass();
        }
        return result;
    }

    private static boolean hasAnnotation(Field field, String simpleName) {
        for (Annotation annotation : field.getDeclaredAnnotations()) {
            if (simpleName.equals(annotation.annotationType().getSimpleName())) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, String> loadDefaultOverrides(Path path) throws IOException {
        JsonObject overrides = readObject(path);
        Map<String, String> result = new HashMap<String, String>();
        for (JsonElement element : overrides.getAsJsonArray("defaults")) {
            JsonObject value = element.getAsJsonObject();
            result.put(
                value.get("rustType").getAsString() + "#" + value.get("field").getAsString(),
                value.get("semantic").getAsString()
            );
        }
        return result;
    }

    private static JsonObject readObject(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return GSON.fromJson(reader, JsonObject.class);
        }
    }

    private static void writeObject(Path path, JsonObject object) throws IOException {
        Files.createDirectories(path.toAbsolutePath().normalize().getParent());
        try (Writer writer = Files.newBufferedWriter(
            path,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        )) {
            GSON.toJson(object, writer);
            writer.write('\n');
        }
    }

    private static Path requiredPath(String name) {
        return Paths.get(requiredProperty(name)).toAbsolutePath().normalize();
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("missing system property: " + name);
        }
        return value;
    }

    private static final class FieldRecord {
        private final String key;
        private final String javaType;
        private final String presence;
        private final String defaultSemantic;
        private final String declaredIn;
        private final int inheritanceDepth;

        private FieldRecord(
            String key,
            String javaType,
            String presence,
            String defaultSemantic,
            String declaredIn,
            int inheritanceDepth
        ) {
            this.key = key;
            this.javaType = javaType;
            this.presence = presence;
            this.defaultSemantic = defaultSemantic;
            this.declaredIn = declaredIn;
            this.inheritanceDepth = inheritanceDepth;
        }

        private JsonObject toJson() {
            Map<String, Object> ordered = new LinkedHashMap<String, Object>();
            ordered.put("key", key);
            ordered.put("javaType", javaType);
            ordered.put("presence", presence);
            ordered.put("defaultSemantic", defaultSemantic);
            ordered.put("declaredIn", declaredIn);
            ordered.put("inheritanceDepth", inheritanceDepth);
            return GSON.toJsonTree(ordered).getAsJsonObject();
        }
    }
}
