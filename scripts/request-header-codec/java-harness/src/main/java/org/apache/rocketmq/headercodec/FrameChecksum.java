/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.rocketmq.headercodec;

/** Deterministic checksums used by cross-language golden fixtures. */
public final class FrameChecksum {
    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private FrameChecksum() {
    }

    public static String fnv1a64(byte[] bytes) {
        long hash = FNV_OFFSET_BASIS;
        for (byte value : bytes) {
            hash ^= value & 0xffL;
            hash *= FNV_PRIME;
        }
        return String.format("%016x", hash);
    }
}
