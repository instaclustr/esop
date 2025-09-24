package com.instaclustr.esop.impl.interaction;

import java.util.Map.Entry;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.esop.impl.KeyspaceTable;
import jmx.org.apache.cassandra.service.CassandraJMXService;

import static java.util.stream.Collectors.toMap;

public class CassandraTables implements CassandraInteraction<Multimap<String, String>> {

    private final CassandraJMXService cassandraJMXService;

    public CassandraTables(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public Multimap<String, String> act() throws Exception {
        return Multimaps.forMap(cassandraJMXService.getCFSMBeans().entries().stream().collect(toMap(Entry::getKey, entry -> entry.getValue().getTableName())));
    }

    public static boolean containsTable(final CassandraJMXService cassandraJMXService, final KeyspaceTable keyspaceTable) throws Exception {
        return new CassandraTables(cassandraJMXService).act().get(keyspaceTable.keyspace).contains(keyspaceTable.table);
    }

    public static boolean containsTable(final CassandraJMXService cassandraJMXService, final String table) throws Exception {
        return new CassandraTables(cassandraJMXService).act().entries().stream().anyMatch(e -> e.getValue().contains(table));
    }
}
