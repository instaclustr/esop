package com.instaclustr.cassandra.backup.impl.restore;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import picocli.CommandLine;

public class RestorationEntities {

    private final List<String> keyspaces;
    private final Multimap<String, String> keyspacesAndTables;

    public RestorationEntities() {
        this.keyspaces = new ArrayList<>();
        this.keyspacesAndTables = HashMultimap.create();
    }

    public RestorationEntities(final List<String> keyspaces,
                               final Multimap<String, String> keyspacesAndTables) {
        this.keyspaces = keyspaces;
        this.keyspacesAndTables = keyspacesAndTables;
    }

    public boolean contains(final String keyspace) {
        return keyspaces.contains(keyspace);
    }

    public boolean contains(final String keyspace, final String table) {
        return contains(keyspace) || keyspacesAndTables.containsEntry(keyspace, table);
    }

    public boolean tableSubsetOnly() {
        return keyspaces.isEmpty();
    }

    @JsonCreator
    public static RestorationEntities parse(final String entities) {

        if (entities == null || entities.trim().isEmpty()) {
            return new RestorationEntities();
        }

        final String sanitizedEntities = entities.replace("\\s+", "");

        final String[] keyspaceTablePairs = sanitizedEntities.split(",");

        final List<String> keyspaces = new ArrayList<>();
        final Multimap<String, String> keyspacesWithTables = HashMultimap.create();

        for (final String ktp : keyspaceTablePairs) {
            if (ktp.contains(".")) {
                final String[] pair = ktp.split(".");
                if (pair.length == 2) {
                    keyspacesWithTables.put(pair[0], pair[1]);
                }
            } else {
                keyspaces.add(ktp);
            }
        }

        return new RestorationEntities(keyspaces, keyspacesWithTables);
    }

    public static RestorationEntities empty() {
        return new RestorationEntities();
    }

    public static class RestorationEntitiesConverter implements CommandLine.ITypeConverter<RestorationEntities> {

        @Override
        public RestorationEntities convert(final String value) throws Exception {

            if (value == null) {
                return new RestorationEntities();
            }

            return RestorationEntities.parse(value);
        }
    }
}
