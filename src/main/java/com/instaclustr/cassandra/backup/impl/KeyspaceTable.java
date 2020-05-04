package com.instaclustr.cassandra.backup.impl;

import com.google.common.base.MoreObjects;

public class KeyspaceTable {

    public enum TableType {
        SYSTEM,
        SYSTEM_AUTH,
        SCHEMA,
        OTHER
    }

    public final String keyspace;
    public final String table;
    public final TableType tableType;

    public KeyspaceTable(final String keyspace, final String table) {
        this.keyspace = keyspace;
        this.table = table;
        this.tableType = classifyTable(keyspace, table);
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
}
