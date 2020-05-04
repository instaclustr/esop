package com.instaclustr.cassandra.backup.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.base.MoreObjects;

public class ManifestEntry {

    public enum Type {
        FILE,
        MANIFEST_FILE,
        CQL_SCHEMA
    }

    public final Path objectKey, localFile;
    public long size;
    public final Type type;
    public final KeyspaceTable keyspaceTable;

    public ManifestEntry(final Path objectKey,
                         final Path localFile,
                         final Type type) throws IOException {
        this(objectKey, localFile, type, null);
    }

    public ManifestEntry(final Path objectKey,
                         final Path localFile,
                         final Type type,
                         final KeyspaceTable keyspaceTable) throws IOException {
        this(objectKey, localFile, type, Files.size(localFile), keyspaceTable);
    }

    public ManifestEntry(final Path objectKey,
                         final Path localFile,
                         final Type type,
                         final long size,
                         final KeyspaceTable keyspaceTable) {
        this.objectKey = objectKey;
        this.localFile = localFile;
        this.size = size;
        this.type = type;
        this.keyspaceTable = keyspaceTable;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("objectKey", objectKey.toAbsolutePath())
            .add("localFile", localFile.toAbsolutePath().toString())
            .add("keyspaceTable", keyspaceTable)
            .add("type", type)
            .add("size", size)
            .toString();
    }
}
