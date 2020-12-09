package com.instaclustr.esop.impl;

import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;

import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.instaclustr.esop.impl.RenamedEntities.Renamed;

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

    public static CassandraData parse(final Path cassandraDir) throws Exception {

        if (!Files.exists(cassandraDir)) {
            return CassandraData.empty();
        }

        final Map<Path, List<Path>> dataDirs = Files.find(cassandraDir,
                                                          2,
                                                          (path, basicFileAttributes) -> basicFileAttributes.isDirectory() &&
                                                              !path.getParent().equals(cassandraDir) &&
                                                              !path.equals(cassandraDir))
            // take only these into consideration which do not have "snapshots/dropped-"
            .filter(table -> {
                try {
                    return Files.find(table, 2, (p, b) -> b.isDirectory() && p.toString().contains("snapshots/dropped-")).count() == 0;
                } catch (final Exception ex) {
                    return false;
                }

            })
            .collect(groupingBy(Path::getParent));

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
        if (entities.areEmpty()) {
            this.databaseEntities = new DatabaseEntities();
        }

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

    /**
     * We need to truncate tables in database entities but not these which acts as "from" in renamed entities
     *
     * If we have ks1.tb1 in db entities and there is ks1.tb1=ks1.tb2 in renamed entities, we actually do not touch ks1.tb1 at all,
     * we will download data for ks1.tb1, we do NOT truncate ks1.tb1 (but we have to truncate ks1.tb2), we clean truncated data after import for
     * ks1.tb2.
     *
     * Lastly, we will filter out all system-related keyspaces and tables from truncation.
     * @param manifest
     */
    public DatabaseEntities getDatabaseEntitiesToProcessForRestore(final Manifest manifest) {
        // this already does not contain system entities
        // and it contains both keyspaces and keyspaceTables
        final DatabaseEntities dbEntitiesView = toDatabaseEntities();

        final DatabaseEntities entities = new DatabaseEntities(dbEntitiesView);

        if (!this.databaseEntities.getKeyspaces().isEmpty()) {
            // remove all other keyspaces except ones we provided via --entities
            entities.retainAll(this.databaseEntities.getKeyspaces());
        }

        if (!this.databaseEntities.getKeyspacesAndTables().isEmpty()) {
            entities.retainAll(this.databaseEntities.getKeyspacesAndTables());
        }

        // remove from dbEntitiesView these which are in "from" in renamed and return,
        // it does not make sense to truncate table we are going to restore
        // under a different name, just keep it untouched
        renamedEntities.getRenamed().forEach(renamed -> entities.remove(renamed.from.fromKeyspace, renamed.from.fromTable));
        renamedEntities.getRenamed().forEach(renamed -> entities.add(renamed.to.toKeyspace, renamed.to.toTable));

        // remove from entities to truncate these which are not in manifest
        // because if we truncated it, we would not have any data to restore from

        final Multimap<String, String> missingEntitiesInManifest = HashMultimap.create();

        entities.getKeyspacesAndTables().entries().forEach(entry -> {
            if (!manifest.getSnapshot().containsTable(entry.getKey(), entry.getValue())) {
                missingEntitiesInManifest.put(entry.getKey(), entry.getValue());
            }
        });

        for (final Map.Entry<String, String> entry : missingEntitiesInManifest.entries()) {
            entities.remove(entry.getKey(), entry.getValue());
        }

        return entities;
    }

}
