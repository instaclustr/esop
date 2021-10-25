package com.instaclustr.esop;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import com.instaclustr.esop.impl.CassandraData;
import com.instaclustr.esop.impl.KeyspaceTable;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.RenamedEntities.Renamed.To;
import com.instaclustr.esop.impl.SSTableUtils;

public class ManifestEnricher {

    private static class TableName {

        public String keyspace;
        public String table;
        public String tableId;

        public TableName(final String keyspace, final String table, final String tableId) {
            this.keyspace = keyspace;
            this.table = table;
            this.tableId = tableId;
        }
    }

    private TableName getTableName(final CassandraData data,
                                   final String keyspace,
                                   final String table,
                                   final String idInManifest,
                                   final boolean forInPlaceStrategy) {
        final Optional<To> renamedTo = data.getRenamedEntities().getRenamedTo(keyspace, table);

        if (renamedTo.isPresent()) {
            final String renamedToKeyspace = renamedTo.get().toKeyspace;
            final String renamedToTable = renamedTo.get().toTable;

            final Optional<String> renamedTableId = data.getTableId(renamedToKeyspace, renamedToTable);

            if (renamedTableId.isPresent()) {
                return new TableName(renamedToKeyspace, renamedToTable, renamedTableId.get());
            }

            throw new IllegalStateException(String.format("Table %s.%s is renamed to %s.%s but there is not table id for renamed table!",
                                                          keyspace, table, renamedToKeyspace, renamedToTable));
        } else {
            final Optional<String> tableId = data.getTableId(keyspace, table);
            if (tableId.isPresent()) {
                return new TableName(keyspace, table, tableId.get());
            } else if (forInPlaceStrategy) {
                return new TableName(keyspace, table, idInManifest);
            }

            throw new IllegalStateException(String.format("Unable to find table id for %s.%s", keyspace, table));
        }
    }

    public void enrich(final CassandraData cassandraData,
                       final Manifest manifest,
                       final Path localRootPath) {

        // populate localFile and keyspaceTable for each entry, based on what
        // table and its id we have locally, ids might differ in manifest from these which are
        // present locally (for example when a table is deleted and then created again and after that
        // we want to e.g. restore - id of that table would be suddenly different from one which was
        // backed up, so we would have problems upon e.g. hard-linking.
        manifest.getSnapshot().forEachEntry((entry, keyspace, table, idInManifest) -> {
            final TableName tableName = getTableName(cassandraData, keyspace, table, idInManifest, false);

            // "data/system/sstable_activity-5a1ff267ace03f128563cfae6103c65e/1-937685388/na-1-big-Filter.db"
            // "data/system/sstable_activity-5a1ff267ace03f128563cfae6103c65e/.indexname/1-937685388/na-1-big-Filter.db"

            final int subPathEndIndex = SSTableUtils.isSecondaryIndexManifest(entry.objectKey) ? 4 : 3;

            Path initialPath = localRootPath == null ? Paths.get(tableName.keyspace) : localRootPath.resolve(tableName.keyspace);

            if (subPathEndIndex == 4) {
                Path indexName = entry.objectKey.subpath(0, 4).getFileName();

                entry.localFile = initialPath
                    .resolve(tableName.table + "-" + tableName.tableId)
                    .resolve(indexName)
                    .resolve(entry.objectKey.getFileName());
            } else {
                entry.localFile = initialPath
                    .resolve(tableName.table + "-" + tableName.tableId)
                    .resolve(entry.objectKey.getFileName());
            }

            entry.keyspaceTable = new KeyspaceTable(tableName.keyspace, tableName.table);
        });

        // rename ids for tables to reflect what we have locally
        manifest.getSnapshot().forEachKeyspace(keyspaceEntry -> {
            keyspaceEntry.getValue().forEachTable(tableEntry -> {
                cassandraData.getTableId(keyspaceEntry.getKey(), tableEntry.getKey())
                    .ifPresent(id -> tableEntry.getValue().setId(id));
            });
        });
    }
}
