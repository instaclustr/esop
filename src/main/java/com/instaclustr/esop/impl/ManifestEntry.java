package com.instaclustr.esop.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class ManifestEntry implements Cloneable {

    public enum Type {
        FILE,
        MANIFEST_FILE,
        CQL_SCHEMA,
        COMMIT_LOG
    }

    private static final class ObjectKeySerializer extends JsonSerializer<Path> {

        @Override
        public void serialize(final Path value, final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
            gen.writeString(value.toString());
        }
    }

    @JsonSerialize(using = ObjectKeySerializer.class)
    public Path objectKey;

    @JsonIgnore
    public Path localFile;

    public long size;

    public Type type;

    public String hash;

    @JsonIgnore
    public String kmsKeyId;

    @JsonIgnore
    public KeyspaceTable keyspaceTable;

    public ManifestEntry(final Path objectKey,
                         final Path localFile,
                         final Type type,
                         final String hash,
                         final String kmsKeyId) {
        this(objectKey, localFile, type, hash, null, kmsKeyId);
    }

    public ManifestEntry(final Path objectKey,
                         final Path localFile,
                         final Type type,
                         final String hash,
                         final KeyspaceTable keyspaceTable,
                         final String kmsKeyId) {
        this(objectKey, localFile, type, 0, keyspaceTable, hash, kmsKeyId);
    }

    @JsonCreator
    public ManifestEntry(@JsonProperty("objectKey") final Path objectKey,
                         @JsonProperty("localFile") final Path localFile,
                         @JsonProperty("type") final Type type,
                         @JsonProperty("size") final long size,
                         @JsonProperty("keyspaceTable") final KeyspaceTable keyspaceTable,
                         @JsonProperty("hash") final String hash,
                         @JsonProperty("kmsKeyId") final String kmsKeyId) {
        this.objectKey = objectKey;
        this.localFile = localFile;
        this.type = type;
        this.keyspaceTable = keyspaceTable;
        this.hash = hash;
        this.kmsKeyId = kmsKeyId;

        try {
            if (size == 0) {
                if (Files.exists(localFile)) {
                    this.size = Files.size(localFile);
                }
            } else {
                this.size = size;
            }
        } catch (final Exception ex) {
            throw new IllegalStateException("Can not determine size of file " + localFile);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("objectKey", objectKey == null ? null : objectKey.toString())
            .add("localFile", localFile == null ? null : localFile.toAbsolutePath().toString())
            .add("keyspaceTable", keyspaceTable)
            .add("type", type)
            .add("size", size)
            .add("hash", hash)
            .add("kmsKeyId", kmsKeyId)
            .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ManifestEntry that = (ManifestEntry) o;
        return size == that.size &&
            Objects.equal(objectKey, that.objectKey) &&
            Objects.equal(localFile, that.localFile) &&
            Objects.equal(hash, that.hash) &&
            type == that.type &&
            Objects.equal(keyspaceTable, that.keyspaceTable) &&
            Objects.equal(kmsKeyId, that.kmsKeyId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(objectKey, localFile, type, keyspaceTable, hash, kmsKeyId);
    }

    @Override
    public ManifestEntry clone() throws CloneNotSupportedException {
        return new ManifestEntry(this.objectKey == null ? null : Paths.get(this.objectKey.toString()),
                                 this.localFile == null ? null : Paths.get(this.localFile.toString()),
                                 this.type,
                                 this.size,
                                 this.keyspaceTable == null ? null : this.keyspaceTable.clone(),
                                 this.hash,
                                 this.kmsKeyId);
    }
}
