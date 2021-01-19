package com.instaclustr.esop.impl;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.instaclustr.esop.impl.KeyspaceTable.KeyspaceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class DatabaseEntities {

    private static Logger logger = LoggerFactory.getLogger(DatabaseEntities.class);

    private final List<String> keyspaces;
    private final Multimap<String, String> keyspacesAndTables;

    public DatabaseEntities() {
        this.keyspaces = new ArrayList<>();
        this.keyspacesAndTables = HashMultimap.create();
    }

    public DatabaseEntities(final List<String> keyspaces,
                            final Multimap<String, String> keyspacesAndTables) {
        this.keyspaces = keyspaces;
        this.keyspacesAndTables = keyspacesAndTables;
    }

    public DatabaseEntities(final DatabaseEntities entities) {
        this();
        this.keyspaces.addAll(entities.keyspaces);
        this.keyspacesAndTables.putAll(entities.keyspacesAndTables);
    }

    public void add(final String keyspace, final String table) {
        if (!this.keyspaces.contains(keyspace)) {
            this.keyspaces.add(keyspace);
        }
        if (!this.keyspacesAndTables.containsEntry(keyspace, table)) {
            this.keyspacesAndTables.put(keyspace, table);
        }
    }

    public void remove(final String keyspace) {
        this.keyspaces.remove(keyspace);
        this.keyspacesAndTables.removeAll(keyspace);
    }

    public void remove(final String keyspace, final String table) {
        // in loop - remove them all
        while (this.keyspacesAndTables.containsEntry(keyspace, table)) {
            this.keyspacesAndTables.remove(keyspace, table);
        }

        // if there is no table under this keyspace anymore, delete that keyspace itself too
        if (this.keyspacesAndTables.get(keyspace).isEmpty()) {
            this.keyspaces.remove(keyspace);
        }
    }

    public boolean contains(final String keyspace) {
        return keyspaces.contains(keyspace);
    }

    public boolean contains(final String keyspace, final String table) {
        return keyspacesAndTables.containsEntry(keyspace, table);
    }

    public boolean tableSubsetOnly() {
        return keyspaces.isEmpty() && !keyspacesAndTables.isEmpty();
    }

    public boolean keyspacesOnly() {
        return !keyspaces.isEmpty() && keyspacesAndTables.isEmpty();
    }

    public boolean areEmpty() {
        return keyspaces.isEmpty() && keyspacesAndTables.isEmpty();
    }

    public static DatabaseEntities create(final String keyspace, final String table) {
        final Multimap<String, String> keyspaceWithTable = HashMultimap.create();
        keyspaceWithTable.put(keyspace, table);
        return new DatabaseEntities(new ArrayList<>(), keyspaceWithTable);
    }

    public Multimap<String, String> getKeyspacesAndTables() {
        return HashMultimap.create(this.keyspacesAndTables);
    }

    public List<String> getKeyspaces() {
        return Collections.unmodifiableList(keyspaces);
    }

    public static DatabaseEntities fromKeyspaceTables(final List<KeyspaceTable> keyspaceTables) {
        final Multimap<String, String> keyspacesAndtables = HashMultimap.create();

        for (final KeyspaceTable kt : keyspaceTables) {
            keyspacesAndtables.put(kt.keyspace, kt.table);
        }

        return new DatabaseEntities(Collections.emptyList(), keyspacesAndtables);
    }

    public static String[] forTakingSnapshot(final DatabaseEntities entities) {
        if (!entities.keyspaces.isEmpty()) {
            return entities.keyspaces.toArray(new String[0]);
        } else if (!entities.keyspacesAndTables.isEmpty()) {
            return entities.keyspacesAndTables.entries().stream().map(entry -> entry.getKey() + "." + entry.getValue()).toArray(String[]::new);
        } else {
            throw new IllegalStateException("Unable to prepare entities for taking a snapshot");
        }
    }

    @Override
    public String toString() {
        return "DatabaseEntities{" +
            "keyspaces=" + keyspaces +
            ", keyspacesAndTables=" + keyspacesAndTables +
            '}';
    }

    @JsonCreator
    public static DatabaseEntities parse(final String entities) {
        if (entities == null || entities.trim().isEmpty()) {
            return DatabaseEntities.empty();
        }

        final String sanitizedEntities = entities.replaceAll("[ ]+", "");

        final String[] keyspaceTablePairs = sanitizedEntities.split(",");

        final List<String> keyspaces = new ArrayList<>();
        final Multimap<String, String> keyspacesWithTables = HashMultimap.create();

        for (final String ktp : keyspaceTablePairs) {
            if (ktp.contains(".")) {
                final String[] pair = ktp.split("\\.");
                if (pair.length == 2) {
                    keyspacesWithTables.put(pair[0], pair[1]);
                }
            } else {
                keyspaces.add(ktp);
            }
        }

        return new DatabaseEntities(keyspaces, keyspacesWithTables);
    }

    public static void validateForRequest(final DatabaseEntities entities) {
        if (entities.areEmpty()) {
            return;
        }

        if (!entities.getKeyspaces().isEmpty() && !entities.getKeyspacesAndTables().isEmpty()) {
            throw new IllegalStateException("Entities might be either set only as keyspaces or only as keyspaces with tables!");
        }
    }

    public static DatabaseEntities empty() {
        return new DatabaseEntities();
    }

    // only used in InPlace strategy
    public DatabaseEntities filter(final DatabaseEntities entitiesFromRequest,
                                   final boolean systemEntities,
                                   final boolean newCluster) {
        if (entitiesFromRequest.areEmpty()) {
            return this;
        }

        if (!entitiesFromRequest.getKeyspaces().isEmpty()) {

            final List<String> keyspaces = entitiesFromRequest
                .getKeyspaces()
                .stream()
                .filter(ks -> getKeyspaces().contains(ks))
                .filter(ks -> {
                    if (KeyspaceTable.isSystemKeyspace(ks)) {
                        if (newCluster && KeyspaceTable.isBootstrappingKeyspace(ks)) {
                            return true;
                        } else {
                            return systemEntities;
                        }
                    }

                    return true;
                })
                .collect(toList());

            final HashMultimap<String, String> keyspacesAndTables = HashMultimap.create();

            getKeyspacesAndTables()
                .entries()
                .stream()
                .filter(entry -> keyspaces.contains(entry.getKey()))
                .forEach(entry -> keyspacesAndTables.put(entry.getKey(), entry.getValue()));

            return new DatabaseEntities(keyspaces, keyspacesAndTables);
        }

        if (!entitiesFromRequest.getKeyspacesAndTables().isEmpty()) {

            final HashMultimap<String, String> map = HashMultimap.create();

            entitiesFromRequest.getKeyspacesAndTables().entries()
                .stream()
                .filter(entry -> getKeyspacesAndTables().containsEntry(entry.getKey(), entry.getValue()))
                .filter(entry -> {
                    if (!systemEntities) {
                        return !KeyspaceTable.isSystemKeyspace(entry.getKey());
                    }
                    return true;
                })
                .forEach(entry -> map.put(entry.getKey(), entry.getValue()));

            return new DatabaseEntities(Collections.emptyList(), map);
        }

        throw new IllegalStateException("Unable to filter entities!");
    }

    public DatabaseEntities filter(final DatabaseEntities entitiesFromRequest,
                                   final boolean systemEntities) {
        return filter(entitiesFromRequest, systemEntities, false);
    }

    public DatabaseEntities removeSystemEntities() {
        final DatabaseEntities databaseEntities = new DatabaseEntities(this);

        final List<String> systemKeyspaces = databaseEntities.keyspaces
            .stream()
            .filter(ks -> KeyspaceTable.classifyKeyspace(ks).equals(KeyspaceType.SYSTEM))
            .collect(toList());

        databaseEntities.keyspaces.removeAll(systemKeyspaces);

        for (final String systemKeyspace : systemKeyspaces) {
            databaseEntities.keyspacesAndTables.removeAll(systemKeyspace);
        }

        return databaseEntities;
    }

    /**
     * Removes all keyspaces except keyspaces in argument, affects both keyspaces and keyspaces and tables structures.
     *
     * @param keyspaces keyspaces to leave untouched
     */
    public void retainAll(final List<String> keyspaces) {

        if (keyspaces.isEmpty()) {
            return;
        }

        this.keyspaces.retainAll(keyspaces);

        final List<String> keyspacesToDelete = new ArrayList<>();

        for (final Map.Entry<String, String> entry : this.keyspacesAndTables.entries()) {
            String keyspace = entry.getKey();

            if (!keyspaces.contains(keyspace)) {
                keyspacesToDelete.add(keyspace);
            }
        }

        for (final String ksToDelete : keyspacesToDelete) {
            this.keyspacesAndTables.removeAll(ksToDelete);
        }
    }

    public void retainAll(final Multimap<String, String> keyspacesAndTables) {
        if (keyspacesAndTables.isEmpty()) {
            return;
        }
        this.keyspaces.clear();
        this.keyspaces.addAll(new ArrayList<>(keyspacesAndTables.keySet()));
        this.keyspacesAndTables.clear();
        this.keyspacesAndTables.putAll(keyspacesAndTables);
    }

    public static class DatabaseEntitiesConverter implements CommandLine.ITypeConverter<DatabaseEntities> {

        @Override
        public DatabaseEntities convert(final String value) throws Exception {
            return DatabaseEntities.parse(value);
        }
    }

    public static class DatabaseEntitiesSerializer extends StdSerializer<DatabaseEntities> {

        public DatabaseEntitiesSerializer() {
            super(DatabaseEntities.class);
        }

        protected DatabaseEntitiesSerializer(final Class<DatabaseEntities> t) {
            super(t);
        }

        @Override
        public void serialize(final DatabaseEntities value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
            if (value != null) {
                if (!value.getKeyspacesAndTables().isEmpty()) {
                    gen.writeString(value.getKeyspacesAndTables().entries().stream().map(entry -> entry.getKey() + "." + entry.getValue()).collect(joining(",")));
                } else if (!value.getKeyspaces().isEmpty()) {
                    gen.writeString(String.join(",", value.getKeyspaces()));
                } else {
                    gen.writeString("");
                }
            }
        }
    }

    public static class DatabaseEntitiesDeserializer extends StdDeserializer<DatabaseEntities> {

        public DatabaseEntitiesDeserializer() {
            super(DatabaseEntities.class);
        }

        @Override
        public DatabaseEntities deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
            final String valueAsString = p.getValueAsString();
            return valueAsString == null ? DatabaseEntities.empty() : DatabaseEntities.parse(valueAsString);
        }
    }
}
