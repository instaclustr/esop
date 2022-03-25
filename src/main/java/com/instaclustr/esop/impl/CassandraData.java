package com.instaclustr.esop.impl;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.instaclustr.esop.impl.RenamedEntities.Renamed;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraData {

    // Map<KeyspaceName,Map<TableName,TableId>>
    private final Map<String, Map<String, String>> tableIdsMap = new HashMap<>();
    // Map<PathToKeyspace,List<PathToTable>>
    private final Map<Path, List<Path>> fullPathsMap = new HashMap<>();

    private RenamedEntities renamedEntities;
    private DatabaseEntities databaseEntities;

    public CassandraData(final Map<String, Map<String, String>> tableIdsMap,
                         final Map<Path, List<Path>> fullPathsMap) {
        this.tableIdsMap.putAll(tableIdsMap);
        this.fullPathsMap.putAll(fullPathsMap);
    }

    public static Set<Path> getLocalDataFiles(final List<Path> dataDirs) {
        return dataDirs.stream()
                       .flatMap(dataDir -> getLocalDataFiles(dataDir).stream())
                       .collect(Collectors.toSet());
    }

    public static Set<Path> getLocalDataFiles(final Path dataDir) {
        final Set<Path> existingEntries = new HashSet<>();
        final int skipBackupsAndSnapshotsFolders = 4;

        if (dataDir.toFile().exists()) {
            try (Stream<Path> paths = Files.walk(dataDir, skipBackupsAndSnapshotsFolders)) {
                paths.filter(Files::isRegularFile).forEach(existingEntries::add);
            } catch (final IOException ex) {
                throw new IllegalStateException(format("Unable to walk through Cassandra data dir %s", dataDir), ex);
            }
        }

        return existingEntries;
    }

    public static boolean containsFile(final Set<Path> localDataFiles,
                                       final Path manifestLocalFile) {
        return localDataFiles.stream()
                             .anyMatch(p -> p.toAbsolutePath().endsWith(manifestLocalFile));
    }

    public boolean containsKeyspace(final String keyspace) {
        return tableIdsMap.get(keyspace) != null;
    }

    public boolean containsTable(final String keyspace, final String table) {
        return tableIdsMap.get(keyspace) != null && tableIdsMap.get(keyspace).get(table) != null;
    }

    public Optional<String> getTableId(final String keyspace, final String table) {
        return Optional.ofNullable(tableIdsMap.get(keyspace))
            .flatMap(tableIds -> Optional.ofNullable(tableIds.get(table)));
    }

    public Optional<List<String>> getTablesNames(final String keyspace) {
        return Optional.ofNullable(tableIdsMap.get(keyspace))
            .map(tableIdMap -> new ArrayList<>(tableIdMap.keySet()));
    }

    public Optional<List<Path>> getTablesPaths(final String keypace) {
        return fullPathsMap
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().getFileName().toString().equals(keypace))
            .map(Entry::getValue)
            .findFirst();
    }

    public Optional<Path> getTablePath(final String keyspace, final String table) {
        Optional<String> tableId = getTableId(keyspace, table);

        if (!tableId.isPresent()) {
            return Optional.empty();
        }

        return fullPathsMap
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().getFileName().toString().equals(keyspace))
            .map(Entry::getValue)
            .flatMap(Collection::stream)
            .filter(tablePath -> tablePath.getFileName().toString().equals(table + "-" + tableId.get()))
            .findFirst();
    }

    public List<Path> getKeyspacePaths() {
        return new ArrayList<>(fullPathsMap.keySet());
    }

    public Optional<Path> getKeyspacePath(final String keyspace) {
        return fullPathsMap.keySet().stream().filter(ksPath -> ksPath.getFileName().toString().equals(keyspace)).findFirst();
    }

    public List<String> getKeyspaceNames() {
        return Collections.unmodifiableList(new ArrayList<>(tableIdsMap.keySet()));
    }

    public static List<Path> listDirs(final Path root,
                                      final Predicate<Path> filter) throws Exception {
        final List<Path> dirs = new ArrayList<>();

        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                if (filter.test(dir)) {
                    dirs.add(dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }

                return FileVisitResult.CONTINUE;
            }
        });

        return dirs;
    }

    public static List<Path> list(final Path root) throws Exception {
        final List<Path> files = new ArrayList<>();

        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {

                if (!attrs.isDirectory()) {
                    files.add(file);
                }

                return FileVisitResult.CONTINUE;
            }
        });

        return files;
    }

    public DatabaseEntities toDatabaseEntities(final boolean includeSystem) {
        final DatabaseEntities databaseEntities = new DatabaseEntities();

        for (final Map.Entry<String, Map<String, String>> entry : tableIdsMap.entrySet()) {
            for (String table : entry.getValue().keySet()) {
                databaseEntities.add(entry.getKey(), table);
            }
        }

        if (!includeSystem) {
            return databaseEntities.removeSystemEntities();
        }

        return databaseEntities;
    }

    public DatabaseEntities toDatabaseEntities() {
        return toDatabaseEntities(false);
    }

    public RenamedEntities getRenamedEntities() {
        return renamedEntities;
    }

    public static class SnapshotsLister extends SimpleFileVisitor<Path> {

        private boolean isDropped = false;

        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            if (dir.getParent().getFileName().toString().equals("snapshots")) {
                if (dir.getFileName().toString().startsWith("dropped-")) {
                    isDropped = true;
                    return FileVisitResult.TERMINATE;
                } else {
                    return FileVisitResult.SKIP_SUBTREE;
                }
            } else {
                return FileVisitResult.CONTINUE;
            }
        }

        public boolean isDropped() {
            return isDropped;
        }
    }

    public static class KeyspaceTableLister extends SimpleFileVisitor<Path> {

        private final Path cassandraDir;
        private final Map<Path, List<Path>> dataDirs = new HashMap<>();

        public KeyspaceTableLister(final Path cassandraDir) {
            this.cassandraDir = cassandraDir;
        }

        private static final Logger logger = LoggerFactory.getLogger(CassandraData.class);

        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            // we hit keyspace
            if (dir.getParent().equals(cassandraDir)) {
                dataDirs.putIfAbsent(dir, new ArrayList<>());
                return FileVisitResult.CONTINUE;
                // we hit table
            } else if (dir.getParent().getParent().equals(cassandraDir)) {
                // detect if it is a dropped table
                Path snapshotsDir = dir.resolve("snapshots");

                if (Files.exists(snapshotsDir)) {
                    SnapshotsLister snapshotsLister = new SnapshotsLister();
                    Files.walkFileTree(snapshotsDir, snapshotsLister);
                    if (snapshotsLister.isDropped()) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }

                String foundTableDir = dir.getFileName().toString().split("-")[0];

                // find tables with same names but different ids if we ever hit a case that
                // there are two tables but one of them is active from Cassandra point of view and the other one
                // is just sitting there being empty without dropped snapshot, we need to make a difference
                // which one takes precedence and in most cases it is the one which has more recent modified timestamp
                Optional<Path> existingDir = dataDirs.get(dir.getParent())
                        .stream()
                        .filter(p -> p.getFileName().toString().split("-")[0].equals(foundTableDir))
                        .findFirst();

                if (existingDir.isPresent()) {
                    long existingLastModified = existingDir.get().toFile().lastModified();
                    long foundLastModified = dir.toFile().lastModified();

                    if (existingLastModified < foundLastModified) {
                        dataDirs.get(dir.getParent()).remove(existingDir.get());
                        dataDirs.get(dir.getParent()).add(dir);
                    }
                } else {
                    dataDirs.get(dir.getParent()).add(dir);
                }

                return FileVisitResult.SKIP_SUBTREE;
            } else {
                return FileVisitResult.CONTINUE;
            }
        }

        /**
         * Remove keyspaces which have 0 tables, it means that each table has a snapshot with "dropped-" snapshot name
         */
        public void removeDroppedKeyspaces() {
            final List<Path> droppedKeyspaces = dataDirs
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().isEmpty())
                .map(Entry::getKey)
                .collect(toList());

            for (final Path droppedKeyspace : droppedKeyspaces) {
                dataDirs.remove(droppedKeyspace);
            }
        }

        public Map<Path, List<Path>> getDataDirs() {
            return dataDirs;
        }

        @Override
        public String toString() {
            return dataDirs.toString();
        }
    }

    public static CassandraData parse(final Path cassandraDir) throws Exception {
        if (!Files.exists(cassandraDir)) {
            return CassandraData.empty();
        }

        final KeyspaceTableLister lister = new KeyspaceTableLister(cassandraDir);

        Files.walkFileTree(cassandraDir, lister);
        lister.removeDroppedKeyspaces();

        final Map<Path, List<Path>> dataDirs = lister.getDataDirs();

        final Map<String, Map<String, String>> tableIdsMap = new HashMap<>();

        for (final Map.Entry<Path, List<Path>> entry : dataDirs.entrySet()) {
            final String keyspace = entry.getKey().getFileName().toString();
            final Map<String, String> idsMap = new HashMap<>();

            for (final Path table : entry.getValue()) {
                final String tableWithId = table.getFileName().toString();
                final String tableName = tableWithId.substring(0, tableWithId.lastIndexOf("-"));
                final String tableId = tableWithId.substring(tableName.length() + 1);

                idsMap.put(tableName, tableId);
            }

            tableIdsMap.put(keyspace, idsMap);
        }

        return new CassandraData(tableIdsMap, dataDirs);
    }

    private static CassandraData empty() {
        return new CassandraData(Collections.emptyMap(), Collections.emptyMap());
    }

    public void setRenamedEntitiesFromRequest(final Map<String, String> renamedEntities) {
        this.setRenamedEntitiesFromRequest(RenamedEntities.parse(renamedEntities));
    }

    public void setRenamedEntitiesFromRequest(final RenamedEntities renamedEntities) {
        // each renamed entity has to be present on disk - origin as well as target

        for (final Renamed renamed : renamedEntities.getRenamed()) {
            if (tableIdsMap.get(renamed.from.fromKeyspace) == null) {
                throw new IllegalStateException(format("There is not keyspace %s to rename an entity from!",
                                                       renamed.from.fromKeyspace));
            }

            if (tableIdsMap.get(renamed.from.fromKeyspace).get(renamed.from.fromTable) == null) {
                throw new IllegalStateException(format("There is not table %s.%s to rename an entity from!",
                                                       renamed.from.fromKeyspace, renamed.from.fromTable));
            }

            if (tableIdsMap.get(renamed.to.toKeyspace) == null) {
                throw new IllegalStateException(format("There is not keyspace %s to rename an entity to!",
                                                       renamed.to.toKeyspace));
            }

            if (tableIdsMap.get(renamed.to.toKeyspace).get(renamed.to.toTable) == null) {
                throw new IllegalStateException(format("There is not table %s.%s to rename an entity to!",
                                                       renamed.to.toKeyspace, renamed.to.toTable));
            }
        }

        this.renamedEntities = renamedEntities;
    }

    public void setDatabaseEntitiesFromRequest(final DatabaseEntities entities) throws IllegalStateException {
        final List<String> notPresentKeyspaces = new ArrayList<>();

        for (final String keyspace : entities.getKeyspaces()) {
            if (!containsKeyspace(keyspace)) {
                notPresentKeyspaces.add(keyspace);
            }
        }

        if (!notPresentKeyspaces.isEmpty()) {
            throw new IllegalStateException(format("Some keyspaces to process are not present in Cassandra: %s", notPresentKeyspaces));
        }

        final List<String> notPresentTables = new ArrayList<>();

        for (final Entry<String, String> keyspaceTable : entities.getKeyspacesAndTables().entries()) {
            if (!containsTable(keyspaceTable.getKey(), keyspaceTable.getValue())) {
                notPresentTables.add(keyspaceTable.getKey() + "." + keyspaceTable.getValue());
            }
        }

        if (!notPresentTables.isEmpty()) {
            throw new IllegalStateException(format("Tables %s to process are not present in Cassandra.", notPresentTables));
        }

        this.databaseEntities = entities;
    }

    // --entities="" --rename=whatever non empty  -> invalid
    // --entities=ks1 --rename=whatever non empty -> invalid
    // --entities=ks1.tb1 --rename=ks1.tb2=ks1.tb2 -> invalid as "from" is not in entities
    // --entities=ks1.tb1 --rename=ks1.tb2=ks1.tb1 -> invalid as "to" is in entities (and from is not in entities)
    // --entities=ks1.tb1 --rename=ks1.tb1=ks1.tb2 -> truncate ks1.tb2 and process just ks1.tb2, k1.tb1 is not touched
    public void validate() {
        // --entities="" --rename=whatever non empty  -> invalid
        if (databaseEntities.areEmpty() && !renamedEntities.areEmpty()) {
            throw new IllegalStateException("database entities are empty but renamed entities are not");
        }

        // --entities=ks1 --rename=whatever non empty -> invalid
        if (databaseEntities.keyspacesOnly() && !renamedEntities.areEmpty()) {
            throw new IllegalStateException("you can not use keyspace entities in connection with rename entities");
        }

        if (databaseEntities.tableSubsetOnly() && !renamedEntities.areEmpty()) {

            for (Renamed renamed : renamedEntities.getRenamed()) {
                // --entities=ks1.tb1 --rename=ks1.tb2=ks1.tb2 -> invalid as "from" is not in entities
                if (!databaseEntities.contains(renamed.from.fromKeyspace, renamed.from.fromTable)) {
                    throw new IllegalStateException(String.format("%s.%s from renamed entity's 'from' part (%s) is not in database entities %s",
                                                                  renamed.from.fromKeyspace,
                                                                  renamed.from.fromTable,
                                                                  renamed.toString(),
                                                                  databaseEntities.toString()));
                }
                // --entities=ks1.tb1 --rename=ks1.tb2=ks1.tb1 -> invalid as "to" is in entities (and from is not in entities)
                if (databaseEntities.contains(renamed.to.toKeyspace, renamed.to.toTable)) {
                    throw new IllegalStateException(String.format("%s.%s from renamed entity's 'to' part (%s) is in database entities %s",
                                                                  renamed.to.toKeyspace,
                                                                  renamed.to.toTable,
                                                                  renamed.toString(),
                                                                  databaseEntities.toString()));
                }
            }
        }
    }

    public DatabaseEntities getDatabaseEntitiesToProcessForRestore() {
        validate();

        if (this.databaseEntities.areEmpty()) {
            return toDatabaseEntities(false);
        }
        if (this.databaseEntities.keyspacesOnly()) {
            final DatabaseEntities entities = toDatabaseEntities(false);
            entities.retainAll(this.databaseEntities.getKeyspaces());
            return entities;
        } else {
            // entities - (rename from) + (rename to)
            //  --entities=ks1.tb1 --rename=ks1.tb1=ks1.tb2 -> ks1.tb2
            //  --entities=ks1.tb1,ks2.tb2 --rename=ks1.tb1=ks1.tb3 -> ks2.tb2,ks1.tb3
            final DatabaseEntities db = new DatabaseEntities(this.databaseEntities);
            for (Renamed renamed : this.renamedEntities.getRenamed()) {
                db.remove(renamed.from.fromKeyspace, renamed.from.fromTable);
            }
            for (Renamed renamed : this.renamedEntities.getRenamed()) {
                db.add(renamed.to.toKeyspace, renamed.to.toTable);
            }
            return db;
        }
    }

    public DatabaseEntities getDatabaseEntitiesToProcessForVerification() {
        validate();

        if (this.databaseEntities.keyspacesOnly()) {
            final DatabaseEntities entities = toDatabaseEntities(false);
            entities.retainAll(this.databaseEntities.getKeyspaces());
            return entities;
        } else {
            return new DatabaseEntities(this.databaseEntities);
        }
    }
}