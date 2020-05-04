package com.instaclustr.cassandra.backup.impl.restore;

import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.KeyspaceTable;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl.ManifestEntry.Type;
import com.instaclustr.cassandra.backup.impl.SSTableUtils;
import com.instaclustr.cassandra.backup.impl._import.ImportOperationRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestorationUtilities {

    private static final Logger logger = LoggerFactory.getLogger(RestorationUtilities.class);

    public static Path resolveLocalManifestPath(final RestoreOperationRequest request) {
        final Path sourceManifest = Paths.get("manifests/" + request.snapshotTag);
        return request.importing.sourceDir.resolve(sourceManifest);
    }

    public static Optional<KeyspaceTable> getKeyspaceTableFromManifestLine(final String manifestLine) {
        final Path manifestPath = getManifestPath(manifestLine);

        if (!manifestPath.getName(0).toString().equals("data")) {
            return Optional.empty(); // don't download non "C*/data" data
        }

        return Optional.of(new KeyspaceTable(manifestPath.getName(1).toString(),
                                             StringUtils.split(manifestPath.getName(2).toString(), '-')[0]));
    }

    private static Path getManifestPath(final String manifestLine) {
        final String[] lineArray = manifestLine.trim().split(" ");

        if (lineArray.length != 2) {
            throw new IllegalArgumentException(format("Invalid snapshot manifest line: %s", manifestLine));
        }

        return Paths.get(lineArray[1]);
    }

    public static Predicate<Path> isSubsetTable(final DatabaseEntities entities) {
        // Subset restore on existing cluster, so only clear out the tables we're restoring
        return p -> {
//            differentiating the paths whenever they contain secondary indexes
            if (isSecondaryIndex(p)) {
                final KeyspaceTable ktSecIndex = new KeyspaceTable(p.getParent().getParent().getParent().getFileName().toString(),
                                                                   StringUtils.split(p.getParent().getParent().getFileName().toString(), "-")[0]);
                return entities.contains(ktSecIndex.keyspace, ktSecIndex.table);
            } else {
                final KeyspaceTable kt = new KeyspaceTable(p.getParent().getParent().getFileName().toString(),
                                                           StringUtils.split(p.getParent().getFileName().toString(), '-')[0]);
                return entities.contains(kt.keyspace, kt.table);
            }
        };
    }

    private static boolean isSecondaryIndex(final Path p) {
        return p.getParent().getFileName().toString().startsWith(".");
    }

    // Ex. manifest line:
    // 3.11: 38 data/test/testuncompressed-ce555490463111e7be3e3d534d5cadea/1-1160807146/mc-1-big-Digest.crc32
    // 2.2:  38 data/test/testuncompressed-37f71aca7dc2383ba70672528af04d4f/1-2632208265/test-testuncompressed-jb-1-Data.db
    // 2.0:  38 data/test/testuncompressed/1-2569865052/test-testuncompressed-jb-1-Data.db
    private static Predicate<String> getManifestFilesAllExceptSystem() {
        return manifestLine -> {
            final Optional<KeyspaceTable> ktOpt = getKeyspaceTableFromManifestLine(manifestLine);

            return ktOpt.map(kt -> kt.tableType != KeyspaceTable.TableType.SYSTEM)
                .orElse(false);
        };
    }

    public static Predicate<String> getManifestFilesForFullExistingRestore(boolean restoreSystemKeyspace) {
        // Full restore on existing cluster, so download:
        // 3.0, 3.1: system_distributed, system_traces, system_schema, system_auth, custom keyspaces
        // 2.0, 2.1, 2.2: system_distributed, system_traces, system_auth, system (only schema_ tables)
        if (restoreSystemKeyspace) {
            return m -> getKeyspaceTableFromManifestLine(m).isPresent();
        }
        return getManifestFilesAllExceptSystem();
    }

    public static Predicate<String> getManifestFilesForFullNewRestore(boolean restoreSystemKeyspace) {
        // Full restore on new cluster, so download:
        // 3.0, 3.1: system_distributed, system_traces, system_schema, system_auth, custom keyspaces
        // 2.0, 2.1, 2.2: system_distributed, system_traces, system_auth, system (only schema_ tables)
        if (restoreSystemKeyspace) {
            return m -> getKeyspaceTableFromManifestLine(m).isPresent();
        }
        return getManifestFilesAllExceptSystem();
    }

    public static Predicate<String> getManifestFilesForSubsetExistingRestore(final DatabaseEntities entities, boolean restoreSystemKeyspace) {
        // Subset restore on existing cluster, so only download subset keyspace.tables.
        // Don't download schema files, so other tables will be unaffected (prefer possibility of PIT subset not matching current schema)
        return m -> getKeyspaceTableFromManifestLine(m)
            .map(kt -> entities.contains(kt.keyspace, kt.table) || (restoreSystemKeyspace && kt.tableType == KeyspaceTable.TableType.SCHEMA))
            .orElse(false);
    }

    // TODO - used in InPlaceRestoration
    public static Predicate<String> getManifestFilesForSubsetNewRestore(final DatabaseEntities entities, boolean restoreSystemKeyspace) {
        // Subset restore on new cluster. Download subset keyspace.tables and:
        // 3.0, 3.1: system_schema, system_auth
        // 2.0, 2.1, 2.2: system_auth, system (only schema_)
        return m -> getKeyspaceTableFromManifestLine(m).map(kt -> kt.tableType == KeyspaceTable.TableType.SYSTEM_AUTH ||
            kt.tableType == KeyspaceTable.TableType.SCHEMA ||
            (restoreSystemKeyspace && kt.tableType == KeyspaceTable.TableType.SCHEMA) ||
            entities.contains(kt.keyspace, kt.table))
            .orElse(false);
    }

    /**
     * Decides whether or not the manifest path includes secondary index files
     *
     * @param manifestPath path to manifest
     * @return true if manifest path includes secondary index files, false otherwise
     */
    public static boolean isSecondaryIndexManifest(final Path manifestPath) {
        // When there's a secondary index, manifest path contains 6 elements (including '.indexName' and 'hashcode')
        // '.indexName' is filtered by subpath(3,4), to avoid the other parts of the manifest path getting misidentified with the '.'
        return manifestPath.getNameCount() == 6 && manifestPath.subpath(3, 4).toString().startsWith(".");
    }

    public static boolean isAnExistingSstable(final Path localPath, final String sstable) {
        try {
            if (localPath.toFile().exists() && SSTableUtils.sstableHash(localPath).equals(sstable)) {
                return true;
            }
        } catch (IOException e) {
            // SSTableUtils.sstableHash may throw exception if SSTable has not been probably downloaded
            logger.error(e.getMessage());
        }
        return false;
    }

    // HELPERS

    public static Path downloadManifest(final RestoreOperationRequest request,
                                        final Restorer restorer,
                                        final String schemaVersion) throws Exception {

        Path downloadedManifest = restorer.downloadFileToDir(request.importing.sourceDir.resolve("manifests"),
                                                             Paths.get("manifests"),
                                                             new ManifestFilteringPredicate(request, schemaVersion));

        Path finalManifestPath = downloadedManifest.getParent().resolve(request.snapshotTag);

        Files.move(downloadedManifest, finalManifestPath);

        return finalManifestPath;
    }

    public static List<ImportOperationRequest> buildImportRequests(final RestoreOperationRequest request, final DatabaseEntities entities) {
        return entities.getKeyspacesAndTables().entries().stream().map(entry -> request.importing.copy(entry.getKey(), entry.getValue())).collect(toList());
    }

    public static List<ManifestEntry> getManifestEntries(final RestoreOperationRequest request) throws Exception {
        return RestorationUtilities.getManifestEntries(request, RestorationUtilities.resolveLocalManifestPath(request));
    }

    public static List<ManifestEntry> getManifestEntriesWithoutSchemaCqls(final RestoreOperationRequest request) throws Exception {
        return getManifestEntries(request).stream().filter(entry -> !entry.localFile.endsWith("schema.cql")).collect(toList());
    }

    public static List<ManifestEntry> getManifestEntriesOnlySchemaCqls(final RestoreOperationRequest request) throws Exception {
        return getManifestEntries(request).stream().filter(entry -> entry.localFile.endsWith("schema.cql")).collect(toList());
    }

    public static List<ManifestEntry> getManifestEntries(final RestoreOperationRequest request, Path manifest) throws Exception {
        final List<String> filteredManifest = getFilteredManifest(request, manifest);

        final List<ManifestEntry> files = filteredManifest.stream().map(line -> {
            final String[] lineArray = line.trim().split(" ");

            final Path manifestPath = Paths.get(lineArray[1]);
            final int hashPathPart = isSecondaryIndexManifest(manifestPath) ? 4 : 3;

            //strip check hash from path
            Path localPath = request.importing.sourceDir.resolve(manifestPath.subpath(0, hashPathPart).resolve(manifestPath.getFileName()));

            if (localPath.getFileName().toString().endsWith("-schema.cql")) {
                localPath = localPath.getParent().resolve("schema.cql");
            }

            Optional<KeyspaceTable> keyspaceTableFromManifestPath = getKeyspaceTableFromManifestLine(line);

            if (keyspaceTableFromManifestPath.isPresent()) {
                return new ManifestEntry(manifestPath, localPath, ManifestEntry.Type.FILE, 0, keyspaceTableFromManifestPath.get());
            }

            return new ManifestEntry(manifestPath, localPath, ManifestEntry.Type.FILE, 0, null);
        }).collect(toList());

        final Optional<Path> tokensFile = getFilteredTokensFile(manifest);

        tokensFile.ifPresent(path -> files.add(new ManifestEntry(path, request.importing.sourceDir.resolve(path), Type.FILE, 0, null)));

        return files;
    }

    public static List<String> getFilteredManifest(final RestoreOperationRequest request, final String manifestLines) throws Exception {
        final List<String> filteredManifest = new ArrayList<>();

        try (final BufferedReader manifestStream = new BufferedReader(new StringReader(manifestLines))) {
            manifestStream.lines()
                .filter(getManifestFilesForSubsetExistingRestore(request.entities, false))
                .collect(toCollection(() -> filteredManifest));
        }

        return filteredManifest;
    }

    public static List<String> getFilteredManifest(final RestoreOperationRequest request, final Path localManifest) throws Exception {
        final List<String> filteredManifest = new ArrayList<>();

        try (final BufferedReader manifestStream = Files.newBufferedReader(localManifest)) {
            manifestStream.lines()
                .filter(getManifestFilesForSubsetExistingRestore(request.entities, false))
                .collect(toCollection(() -> filteredManifest));
        }

        return filteredManifest;
    }

    public static Optional<Path> getFilteredTokensFile(final Path manifest) throws Exception {
        try (final BufferedReader manifestStream = Files.newBufferedReader(manifest)) {
            return manifestStream.lines()
                .filter(line -> line.endsWith("yaml") && line.contains(" tokens/"))
                .map(line -> Paths.get(line.split(" ")[1]))
                .findFirst();
        }
    }

    public static DatabaseEntities getRestorationEntitiesFromManifest(final List<String> filteredManifest) {
        final List<KeyspaceTable> keyspaceTables = filteredManifest.stream()
            .map(RestorationUtilities::getKeyspaceTableFromManifestLine)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(toList());

        return DatabaseEntities.fromKeyspaceTables(keyspaceTables);
    }

    // predicates

    public static abstract class AbstractFilteringPredicate implements Predicate<String> {

        protected final RestoreOperationRequest request;
        protected final String currentSchemaVersion;

        public AbstractFilteringPredicate(final RestoreOperationRequest request, final String currentSchemaVersion) {
            this.request = request;
            this.currentSchemaVersion = currentSchemaVersion;
        }

        protected boolean filter(String toFilterOn) {
            if (request.exactSchemaVersion) {
                if (request.schemaVersion != null) {
                    return toFilterOn.contains(request.schemaVersion.toString());
                } else if (currentSchemaVersion != null) {
                    return toFilterOn.contains(currentSchemaVersion);
                }

                throw new IllegalStateException("exactSchemaVersion is required but there is not schemaVersion is request nor runtime Cassandra version!");
            } else {
                if (request.schemaVersion != null) {
                    return toFilterOn.contains(request.schemaVersion.toString());
                } else if (currentSchemaVersion != null) {
                    return toFilterOn.contains(currentSchemaVersion);
                } else {
                    return true;
                }
            }
        }
    }

    public static class ManifestFilteringPredicate extends AbstractFilteringPredicate {

        public ManifestFilteringPredicate(final RestoreOperationRequest request, final String currentSchemaVersion) {
            super(request, currentSchemaVersion);
        }

        @Override
        public boolean test(final String s) {
            return (s.contains("manifests/" + request.snapshotTag) || s.startsWith(request.snapshotTag)) && filter(s);
        }
    }

    public static class TokensFilteringPredicate extends AbstractFilteringPredicate {

        public TokensFilteringPredicate(final RestoreOperationRequest request, final String currentSchemaVersion) {
            super(request, currentSchemaVersion);
        }

        @Override
        public boolean test(final String s) {
            return (s.contains(request.snapshotTag) && s.endsWith("-tokens.yaml")) && filter(s);
        }
    }
}