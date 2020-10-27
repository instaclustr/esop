package com.instaclustr.esop.impl;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyspaceTable implements Cloneable {

    private static final Logger logger = LoggerFactory.getLogger(KeyspaceTable.class);

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

    private static final List<String> bootstrappingKeyspaces = Arrays.asList("system", "system_schema");

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

    public static void checkEntitiesToProcess(final Path cassandraDataDir,
                                              final DatabaseEntities databaseEntities) {
        try {
            final KeyspaceTables keyspaceTables = KeyspaceTable.parseFileSystem(cassandraDataDir);

            final Optional<Pair<List<String>, Multimap<String, String>>> missingEntities = keyspaceTables.filterNotPresent(databaseEntities);

            if (missingEntities.isPresent()) {
                if (!missingEntities.get().getLeft().isEmpty()) {
                    throw new IllegalStateException(format("Unable to process these keyspaces as they are not present in the database: %s",
                                                           missingEntities.get().getLeft()));
                } else if (!missingEntities.get().getRight().isEmpty()) {
                    throw new IllegalStateException(format("Unable to process these tables as they are not present in the database: %s",
                                                           missingEntities.get().getRight().entries().stream().map(e -> e.getKey() + "." + e.getValue()).collect(joining(","))));
                }
            }
        } catch (final Exception ex) {
            throw new RuntimeException("Unable to check SSTables on disk!", ex);
        }
    }

    public static class KeyspaceTables {

        List<KeyspaceTable> keyspaceTables = new ArrayList<>();

        public boolean contains(String keyspace, String table) {
            return keyspaceTables.stream().anyMatch(kt -> kt.keyspace.equals(keyspace) && kt.table.equals(table));
        }

        public boolean contains(String keyspace) {
            return keyspaceTables.stream().anyMatch(kt -> kt.keyspace.equals(keyspace));
        }

        public void add(final String keyspace, final String table) {
            this.keyspaceTables.add(new KeyspaceTable(keyspace, table));
        }

        public Optional<Pair<List<String>, Multimap<String, String>>> filterNotPresent(final DatabaseEntities dbEntities) {
            if (dbEntities.areEmpty()) {
                return Optional.empty();
            }

            final List<String> missingKeyspaces = new ArrayList<>();
            final Multimap<String, String> missingTables = ArrayListMultimap.create();

            if (dbEntities.tableSubsetOnly()) {
                for (final Entry<String, String> entry : dbEntities.getKeyspacesAndTables().entries()) {
                    if (!contains(entry.getKey(), entry.getValue())) {
                        missingTables.put(entry.getKey(), entry.getValue());
                    }
                }
            } else if (dbEntities.keyspacesOnly()) {
                for (final String keyspace : dbEntities.getKeyspaces()) {
                    if (!contains(keyspace)) {
                        missingKeyspaces.add(keyspace);
                    }
                }
            }

            return Optional.of(Pair.of(missingKeyspaces, missingTables));
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("keyspaceTables", keyspaceTables)
                .toString();
        }
    }

    public static KeyspaceTables parseFileSystem(final Path cassandraDataDir) throws Exception {

        final Set<String> excludedDirs = Stream.of("hints", "commitlog", "cdc_raw", "saved_caches").collect(Collectors.toSet());

        final KeyspaceTables keyspaceTables = new KeyspaceTables();

        final List<Path> keyspaces = Files.find(
            cassandraDataDir,
            1,
            (path, basicFileAttributes) -> !cassandraDataDir.equals(path) && basicFileAttributes.isDirectory())
            .filter(path -> !excludedDirs.contains(path.getFileName().toString())).collect(toList());

        for (final Path ks : keyspaces) {

            Files.find(ks, 1, (path, basicFileAttributes) -> !ks.equals(path) && basicFileAttributes.isDirectory()).collect(toList()).forEach(path -> {

                final String tableNameFull = path.toFile().getName();

                if (tableNameFull.contains("-") && tableNameFull.split("-").length == 2) {
                    final String tableNameSimple = tableNameFull.split("-")[0];
                    keyspaceTables.add(ks.getFileName().toString(), tableNameSimple);
                } else {
                    logger.info(String.format("Skipping directory %s in %s, it does not look like table dir!", tableNameFull, path.toAbsolutePath().toString()));
                }
            });
        }

        logger.info("Found keyspaces and tables: {}", keyspaceTables);

        return keyspaceTables;
    }
}
