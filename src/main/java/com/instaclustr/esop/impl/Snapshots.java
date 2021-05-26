package com.instaclustr.esop.impl;

import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.HashMultimap;
import com.instaclustr.esop.impl.ManifestEntry.Type;
import com.instaclustr.esop.impl.Snapshots.Snapshot.Keyspace.Table;
import com.instaclustr.esop.impl.hash.HashSpec;

public class Snapshots implements Cloneable {

    public static HashSpec hashSpec;

    private final Map<String, Snapshot> snapshots = new HashMap<>();

    public final Optional<Snapshot> get(final String snapshotTag) {
        return Optional.ofNullable(snapshots.get(snapshotTag));
    }

    public boolean isEmpty() {
        return snapshots.isEmpty();
    }

    public int size() {
        return snapshots.size();
    }

    public void clear() {
        snapshots.clear();
    }

    public void add(final String snapshotTag, final Snapshot snapshot) {
        snapshots.put(snapshotTag, snapshot);
    }

    public void add(final Snapshots snapshots) {
        this.snapshots.putAll(snapshots.snapshots);
    }

    @Override
    public Snapshots clone() throws CloneNotSupportedException {
        final Snapshots cloned = new Snapshots();
        for (final Map.Entry<String, Snapshot> snapshot : this.snapshots.entrySet()) {
            cloned.add(snapshot.getKey(), snapshot.getValue().clone());
        }
        return cloned;
    }

    @Override
    public String toString() {
        return "Snapshots{" +
            "snapshots=" + snapshots +
            '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Snapshots snapshots1 = (Snapshots) o;
        return com.google.common.base.Objects.equal(snapshots, snapshots1.snapshots);
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(snapshots);
    }

    public static class Snapshot implements Cloneable {

        private static final Set<String> SYSTEM_KEYSPACES = Stream.of("system", "system_schema", "system_distributed", "system_traces", "system_auth").collect(toSet());

        private String name;

        private final Map<String, Keyspace> keyspaces = new HashMap<>();

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public boolean containsKeyspace(final String keyspace) {
            return keyspaces.entrySet().stream().anyMatch(entry -> entry.getKey().equals(keyspace));
        }

        public boolean containsTable(final String keyspace, final String table) {
            return Optional.ofNullable(keyspaces.get(keyspace)).flatMap(ks -> ks.getTable(table)).isPresent();
        }

        public Optional<Keyspace> getKeyspace(final String keyspace) {
            return Optional.ofNullable(keyspaces.get(keyspace));
        }

        public Optional<Table> getTable(final String keyspace, final String table) {
            return Optional.ofNullable(keyspaces.get(keyspace)).flatMap(ks -> ks.getTable(table));
        }

        public Map<String, Keyspace> getKeyspaces() {
            return Collections.unmodifiableMap(keyspaces);
        }

        public void removeKeyspace(final String keyspace) {
            keyspaces.remove(keyspace);
        }

        public void removeKeyspaces(final List<String> keyspaces) {
            for (final String keyspace : keyspaces) {
                removeKeyspace(keyspace);
            }
        }

        public void removeTables(final String keyspace, final List<String> tables) {
            this.getKeyspace(keyspace).ifPresent(ks -> {
                final Map<String, Table> valuesToStay = ks.getTables().entrySet().stream()
                    .filter(entry -> !tables.contains(entry.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
                ks.setTables(valuesToStay);
            });
        }

        public void removeTable(final String keyspace, final String table) {
            removeTables(keyspace, Collections.singletonList(table));
        }

        public void add(final String name, final Keyspace keyspace) {
            this.keyspaces.put(name, keyspace);
        }

        @Override
        public Snapshot clone() throws CloneNotSupportedException {
            final Snapshot cloned = new Snapshot();
            cloned.setName(this.name);
            for (final Map.Entry<String, Keyspace> keyspace : this.keyspaces.entrySet()) {
                cloned.add(keyspace.getKey(), keyspace.getValue().clone());
            }
            return cloned;
        }

        @JsonIgnore
        public Collection<String> getKeyspaceNames() {
            return Collections.unmodifiableCollection(keyspaces.keySet());
        }

        @JsonIgnore
        public HashMultimap<String, String> getKeyspacesAndTables() {
            return getKeyspacesAndTables(true);
        }

        @JsonIgnore
        public HashMultimap<String, String> getKeyspacesAndTables(final boolean withSystemKeyspaces) {

            final HashMultimap<String, String> ksAndTbls = HashMultimap.create();

            getKeyspaces().entrySet().stream().filter(e -> {
                if (SYSTEM_KEYSPACES.contains(e.getKey())) {
                    return withSystemKeyspaces;
                }

                return true;
            }).forEach(e -> e.getValue().tables.keySet().forEach(t -> ksAndTbls.put(e.getKey(), t)));

            return ksAndTbls;
        }

        @JsonIgnore
        public List<ManifestEntry> getManifestEntries(final String... keyspace) {
            return Collections.unmodifiableList(keyspaces.entrySet().stream()
                                                    .filter(ks -> Arrays.asList(keyspace).contains(ks.getKey()))
                                                    .flatMap(entry -> entry.getValue().getManifestEntries().stream())
                                                    .collect(toList()));
        }

        @JsonIgnore
        public List<ManifestEntry> getManifestEntries() {
            return keyspaces.entrySet().stream().flatMap(keyspace -> keyspace.getValue().getManifestEntries().stream()).collect(toList());
        }

        public static Snapshot parse(final String snapshotName, final List<Path> snapshotPaths) throws Exception {

            final Snapshot snapshot = new Snapshot();
            snapshot.setName(snapshotName);

            final Map<String, List<Path>> keyspaceSnapshotPaths = snapshotPaths.stream()
                .collect(groupingBy(p -> p.getParent().getParent().getParent().getFileName().toString()));

            for (final Entry<String, List<Path>> entry : keyspaceSnapshotPaths.entrySet()) {
                snapshot.keyspaces.put(entry.getKey(), Keyspace.parse(entry.getKey(), entry.getValue()));
            }

            return snapshot;
        }

        @JsonIgnore
        public List<ManifestEntry> getSchemas() {
            return keyspaces.entrySet().stream()
                .flatMap(entry -> entry.getValue().getTables().values().stream().map(Table::getSchema))
                .filter(Objects::nonNull)
                .collect(toList());
        }

        @JsonIgnore
        public Optional<ManifestEntry> getSchema(final String keyspace, final String table) {
            final Optional<Keyspace> ks = Optional.ofNullable(keyspaces.get(keyspace));

            if (!ks.isPresent()) {
                return Optional.empty();
            }

            return ks.flatMap(ksOpt -> ksOpt.getTable(table)).map(Table::getSchema);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Snapshot snapshot = (Snapshot) o;
            return com.google.common.base.Objects.equal(name, snapshot.name) &&
                com.google.common.base.Objects.equal(keyspaces, snapshot.keyspaces);
        }

        @Override
        public String toString() {
            return "Snapshot{" +
                "name='" + name + '\'' +
                ", keyspaces=" + keyspaces +
                '}';
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(name, keyspaces);
        }

        /**
         * This check is relaxed on non-existent keyspaces in the other snapshot.
         * If there is not a keyspace this keyspace has, it is as if they were equal on schemas.
         * There might be snapshots taken against same schema but against different set of keyspaces and tables.
         *
         * @param snapshot snapshot to compare
         * @return true if schemas are same
         */
        public boolean hasSameSchemas(final Snapshot snapshot) {
            for (final Entry<String, Keyspace> ks : keyspaces.entrySet()) {

                if (snapshot.getKeyspace(ks.getKey()).isPresent()) {
                    if (!ks.getValue().hasSameSchema(snapshot.getKeyspace(ks.getKey()).get())) {
                        return false;
                    }
                }
            }

            return true;
        }

        @FunctionalInterface
        public interface ManifestEntryConsumer {

            void consume(ManifestEntry entry, String keyspace, String table);
        }

        @FunctionalInterface
        public interface ManifestEntryIdConsumer {

            void consume(ManifestEntry entry, String keyspace, String table, String tableId);
        }

        public void forEachEntry(final ManifestEntryConsumer consumer) {
            forEachKeyspace(keyspaceEntry -> {
                final String keyspace = keyspaceEntry.getKey();

                keyspaceEntry.getValue().forEachTable(tableEntry -> {
                    final String tableName = tableEntry.getKey();

                    tableEntry.getValue().forEachEntry(manifestEntry -> consumer.consume(manifestEntry, keyspace, tableName));
                });
            });
        }

        public void forEachEntry(final ManifestEntryIdConsumer consumer) {
            forEachKeyspace(keyspaceEntry -> {
                final String keyspace = keyspaceEntry.getKey();

                keyspaceEntry.getValue().forEachTable(tableEntry -> {
                    final String tableId = tableEntry.getValue().id;
                    final String tableName = tableEntry.getKey();

                    tableEntry.getValue().forEachEntry(manifestEntry -> consumer.consume(manifestEntry, keyspace, tableName, tableId));
                });
            });
        }

        public void forEachKeyspace(Consumer<Entry<String, Keyspace>> consumer) {
            this.keyspaces.entrySet().forEach(consumer);
        }

        public static class Keyspace implements Cloneable {

            private final Map<String, Table> tables = new HashMap<>();

            @JsonCreator
            public Keyspace(@JsonProperty("tables") Map<String, Table> tables) {
                if (tables != null) {
                    this.tables.putAll(tables);
                    for (final Map.Entry<String, Table> entry : this.tables.entrySet()) {
                        entry.getValue().setName(entry.getKey());
                    }
                }
            }

            public static Keyspace parse(final String keyspace, List<Path> snapshotPaths) throws Exception {
                final Map<String, List<Path>> tableSnapshotPaths = snapshotPaths.stream().collect(groupingBy(p -> p.getParent().getParent().getFileName().toString()));

                final Map<String, Table> tables = new HashMap<>();

                for (final Entry<String, List<Path>> entry : tableSnapshotPaths.entrySet()) {
                    Table parsedTable = Table.parse(keyspace, entry.getKey(), entry.getValue());
                    tables.put(parsedTable.name, parsedTable);
                }

                return new Keyspace(tables);
            }

            public void forEachTable(Consumer<Entry<String, Table>> consumer) {
                this.tables.forEach((name, table) -> {
                    tables.entrySet().forEach(consumer);
                });
            }

            public Map<String, Table> getTables() {
                return Collections.unmodifiableMap(tables);
            }

            public void setTables(Map<String, Table> tables) {
                this.tables.clear();
                this.tables.putAll(tables);
            }

            public Optional<Table> getTable(String tableName) {
                return Optional.ofNullable(tables.get(tableName));
            }

            public List<ManifestEntry> getManifestEntries(final String... tables) {
                return Collections.unmodifiableList(this.tables.entrySet().stream()
                                                        .filter(entry -> Arrays.asList(tables).contains(entry.getKey()))
                                                        .flatMap(table -> table.getValue().getEntries().stream())
                                                        .collect(toList()));
            }

            @JsonIgnore
            public List<ManifestEntry> getManifestEntries() {
                return Collections.unmodifiableList(tables.entrySet().stream().flatMap(table -> table.getValue().getEntries().stream()).collect(toList()));
            }

            public boolean containsTable(final String table) {
                return tables.entrySet().stream().anyMatch(entry -> entry.getKey().equals(table));
            }

            @JsonIgnore
            public List<ManifestEntry> getAllSchemas() {
                return tables.values().stream().map(Table::getSchema).collect(toList());
            }

            public void add(final String name, final Table table) {
                this.tables.put(name, table);
            }

            /**
             * Returns true if provided keyspace is same on schemas. "Same" here is relaxed,
             * if other keyspace does not contain a table which is in this keyspace,
             * the equality check will not be done. This means that this method returns
             * true also in case number of tables is different for each keyspace but
             * for tables which are same, their schemas match.
             *
             * @param otherKeyspace keyspace to check schemas for
             * @return true if schemas are same, false otherse
             */
            public boolean hasSameSchema(final Keyspace otherKeyspace) {
                return getTablesWithDifferentSchemas(otherKeyspace).isEmpty();
            }

            public List<String> getTablesWithDifferentSchemas(final Keyspace otherKeyspace) {
                final Map<String, String> otherSchemas = otherKeyspace.getTableSchemas();
                final Map<String, String> ourSchemas = getTableSchemas();

                final List<String> differentTables = new ArrayList<>();

                for (final Entry<String, String> ourSchema : ourSchemas.entrySet()) {
                    if (!otherSchemas.containsKey(ourSchema.getKey())) {
                        continue;
                    }

                    if (!otherSchemas.get(ourSchema.getKey()).equals(ourSchema.getValue())) {
                        differentTables.add(ourSchema.getKey());
                    }
                }

                return differentTables;
            }

            public List<String> getTablesNamesWithDifferentSchemas(final Keyspace otherKeyspace) {
                return null;
            }

            @JsonIgnore
            public Map<String, String> getTableSchemas() {
                return this.tables.entrySet().stream()
                    .filter(e -> Objects.nonNull(e.getValue().schemaContent))
                    .collect(toMap(Entry::getKey, e -> e.getValue().schemaContent));
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final Keyspace keyspace = (Keyspace) o;
                return com.google.common.base.Objects.equal(tables, keyspace.tables);
            }

            @Override
            public int hashCode() {
                return com.google.common.base.Objects.hashCode(tables);
            }

            @Override
            public String toString() {
                return "Keyspace{" +
                    "tables=" + tables +
                    '}';
            }

            @Override
            public Keyspace clone() throws CloneNotSupportedException {
                return new Keyspace(new HashMap<>(this.tables));
            }

            public static class Table implements Cloneable {

                public static final Pattern TABLE_PATTERN = Pattern.compile("(.*)-((.){32})");

                private final List<ManifestEntry> entries = new ArrayList<>();
                // simple name without id

                public String name;
                public String id;
                private ManifestEntry schema;
                private String schemaContent;

                public Table() {

                }

                @JsonCreator
                public Table(final @JsonProperty("entries") List<ManifestEntry> entries,
                             final @JsonProperty("id") String id,
                             final @JsonProperty("schemaContent") String schemaContent) {
                    this.entries.addAll(entries);
                    this.schema = entries.stream().filter(entry -> entry.type == Type.CQL_SCHEMA).findFirst().orElse(null);
                    this.schemaContent = schemaContent;
                    this.id = id;
                }

                public static Table parse(final String keyspace, final String table, final List<Path> value) throws Exception {
                    final Table tb = new Table();

                    final Matcher matcher = TABLE_PATTERN.matcher(table);

                    if (matcher.matches()) {
                        tb.setName(matcher.group(1));
                        tb.setId(matcher.group(2));
                    } else {
                        throw new IllegalStateException(format("Illegal format of table name %s for pattern %s", table, TABLE_PATTERN));
                    }

                    final Path tablePath = Paths.get("data").resolve(Paths.get(keyspace, table));

                    for (final Path path : value) {
                        tb.entries.addAll(SSTableUtils.ssTableManifest(path, tablePath, Snapshots.hashSpec).collect(toList()));
                    }

                    final Optional<Path> schemaPath = value.stream().map(p -> p.resolve("schema.cql")).filter(Files::exists).findFirst();

                    if (schemaPath.isPresent()) {
                        final Path schema = schemaPath.get();
                        tb.schema = new ManifestEntry(tablePath.resolve("schema.cql"),
                                                      schema,
                                                      Type.CQL_SCHEMA,
                                                      null);
                        tb.schemaContent = new String(Files.readAllBytes(schemaPath.get()));
                        tb.entries.add(tb.schema);
                    }

                    return tb;
                }

                public void forEachEntry(Consumer<ManifestEntry> entryConsumer) {
                    getEntries().forEach(entryConsumer);
                }

                public List<ManifestEntry> getEntries() {
                    return Collections.unmodifiableList(entries);
                }

                @JsonIgnore
                public String getName() {
                    return name;
                }

                public void setName(final String name) {
                    this.name = name;
                }

                public String getId() {
                    return id;
                }

                public void setId(final String id) {
                    this.id = id;
                }

                @JsonIgnore
                public void add(ManifestEntry manifestEntry) {
                    this.entries.add(manifestEntry);
                }

                @JsonIgnore
                public ManifestEntry getSchema() {
                    return schema;
                }

                public String getSchemaContent() {
                    return schemaContent;
                }

                /**
                 *
                 * @return CQL creation statement without everything after "WITH ..."
                 */
                @JsonIgnore
                public String getSimpleSchema() {
                    return parseSimpleSchema(schemaContent);
                }

                private String parseSimpleSchema(final String fullSchema) {
                    Pattern p = Pattern.compile("(.*)(WITH)(.*)");
                    Matcher m = p.matcher(fullSchema.replaceAll("\\n", ""));
                    if (m.matches()) {
                        return m.group(1);
                    }

                    throw new IllegalStateException("Unable to parse simple schema from schema " + fullSchema);
                }

                public boolean schemaEqualsTo(final Path schemaPath) throws Exception {
                    return schemaEqualsTo(new String(Files.readAllBytes(schemaPath)));
                }

                public boolean schemaEqualsTo(final Table otherTable) {
                    return schemaEqualsTo(otherTable.getSchemaContent());
                }

                public boolean schemaEqualsTo(final String schemaContent) {
                    final String simpleSchema = getSimpleSchema();
                    return simpleSchema != null && simpleSchema.equals(parseSimpleSchema(schemaContent));
                }

                @Override
                public boolean equals(final Object o) {
                    if (this == o) {
                        return true;
                    }
                    if (o == null || getClass() != o.getClass()) {
                        return false;
                    }
                    final Table table = (Table) o;
                    return com.google.common.base.Objects.equal(entries, table.entries) &&
                        com.google.common.base.Objects.equal(name, table.name) &&
                        com.google.common.base.Objects.equal(id, table.id) &&
                        com.google.common.base.Objects.equal(schema, table.schema) &&
                        com.google.common.base.Objects.equal(schemaContent, table.schemaContent);
                }

                @Override
                public int hashCode() {
                    return com.google.common.base.Objects.hashCode(entries, name, id, schema, schemaContent);
                }

                @Override
                public String toString() {
                    return "Table{" +
                        "entries=" + entries +
                        ", name='" + name + '\'' +
                        ", id='" + id + '\'' +
                        ", schema=" + schema +
                        ", schemaContent='" + schemaContent + '\'' +
                        '}';
                }

                @Override
                public Table clone() throws CloneNotSupportedException {
                    final Table cloned = new Table();

                    cloned.setId(this.id);
                    cloned.setName(this.name);

                    if (this.schema != null) {
                        cloned.schema = this.schema.clone();
                    }

                    cloned.schemaContent = schemaContent;

                    for (final ManifestEntry me : this.entries) {
                        cloned.entries.add(me.clone());
                    }

                    return cloned;
                }
            }
        }
    }

    public static synchronized Snapshots parse(final Path cassandraDir) throws Exception {

        if (Snapshots.hashSpec == null) {
            Snapshots.hashSpec = new HashSpec();
        }

        final Snapshots snapshots = new Snapshots();
        final SnapshotLister lister = new SnapshotLister();
        Files.walkFileTree(cassandraDir, lister);

        final Map<String, List<Path>> snapshotPaths = lister.getSnapshotPaths();

        for (final Entry<String, List<Path>> paths : snapshotPaths.entrySet()) {
            snapshots.snapshots.put(paths.getKey(), Snapshot.parse(paths.getKey(), paths.getValue()));
        }

        return snapshots;
    }

    public static boolean snapshotContainsTimestamp(String snapshotTag) {
        // most probably it is of form "snapshot-uuid-timestamp"
        if (snapshotTag.contains("-") && !snapshotTag.startsWith("-") && !snapshotTag.endsWith("-")) {
            try {
                Long.parseLong(snapshotTag.substring(snapshotTag.lastIndexOf("-")));
                return true;
            } catch (final Exception ex) {
                return false;
            }
        }

        return false;
    }

    public static class SnapshotLister extends SimpleFileVisitor<Path> {

        private final List<Path> snapshotPaths = new ArrayList<>();

        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            if (dir.getParent().getFileName().toString().equals("snapshots")) {
                if (!dir.getFileName().toString().startsWith("truncated-")) {
                    if (!dir.getFileName().toString().startsWith("dropped-")) {
                        snapshotPaths.add(dir);
                    } else {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                } else {
                    return FileVisitResult.SKIP_SUBTREE;
                }
            }

            return FileVisitResult.CONTINUE;
        }

        public Map<String, List<Path>> getSnapshotPaths() {
            return snapshotPaths.stream().collect(groupingBy(p -> p.getFileName().toString()));
        }
    }
}
