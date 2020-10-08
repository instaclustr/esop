package com.instaclustr.esop.impl;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.StringUtils;

public class KeyspaceTable implements Cloneable {

    public enum TableType {
        SYSTEM,
        SYSTEM_AUTH,
        SCHEMA,
        OTHER
    }

    public final String keyspace;
    public final String table;
    public final TableType tableType;

    public static Optional<KeyspaceTable> parse(final Path relativePath) {
        if (!relativePath.getName(0).toString().equals("data")) {
            return Optional.empty(); // don't parse non-data files
        }

        return Optional.of(new KeyspaceTable(relativePath.getName(1).toString(),
                                             StringUtils.split(relativePath.getName(2).toString(), '-')[0]));
    }

    @JsonCreator
    public KeyspaceTable(final @JsonProperty("keyspace") String keyspace,
                         final @JsonProperty("table") String table) {
        this.keyspace = keyspace;
        this.table = table;
        this.tableType = classifyTable(keyspace, table);
    }

    public static boolean isSystemKeyspace(final String keyspace) {
        return keyspace.equals("system") || keyspace.equals("system_schema");
    }

    private static final List<String> bootstrappingKeyspaces = Arrays.asList("system_schema");

    public static boolean isBootstrappingKeyspace(final String keyspace) {
        return bootstrappingKeyspaces.contains(keyspace);
    }

    public TableType classifyTable(final String keyspace, final String table) {
        if (keyspace.equals("system") && !table.startsWith("schema_")) {
            return TableType.SYSTEM;
        } else if (keyspace.equals("system_schema") ||
            (keyspace.equals("system") && table.startsWith("schema_"))) {
            return TableType.SCHEMA;
        } else if (keyspace.equals("system_auth")) {
            return TableType.SYSTEM_AUTH;
        } else {
            return TableType.OTHER;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(KeyspaceTable.this)
            .add("keyspace", keyspace)
            .add("table", table)
            .add("tableType", tableType)
            .toString();
    }

    @Override
    public KeyspaceTable clone() throws CloneNotSupportedException {
        return new KeyspaceTable(this.keyspace, this.table);
    }
}
