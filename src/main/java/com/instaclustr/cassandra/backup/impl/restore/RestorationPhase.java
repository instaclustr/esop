package com.instaclustr.cassandra.backup.impl.restore;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.CLUSTER_HEALTHCHECK;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.INIT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.buildImportRequests;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.downloadManifest;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.getFilteredManifest;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.getManifestEntries;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.getManifestEntriesWithoutSchemaCqls;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.getRestorationEntitiesFromManifest;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.resolveLocalManifestPath;
import static com.instaclustr.io.FileUtils.createOrCleanDirectory;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.commons.io.FileUtils.listFilesAndDirs;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl._import.ImportOperation;
import com.instaclustr.cassandra.backup.impl._import.ImportOperationRequest;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraSameTokens;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraState;
import com.instaclustr.cassandra.backup.impl.interaction.ClusterSchemaVersions;
import com.instaclustr.cassandra.backup.impl.interaction.ClusterState;
import com.instaclustr.cassandra.backup.impl.interaction.FailureDetector;
import com.instaclustr.cassandra.backup.impl.refresh.RefreshOperation;
import com.instaclustr.cassandra.backup.impl.refresh.RefreshOperationRequest;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperation;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperationRequest;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationProgressTracker;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Phase of a cluster-wide restoration. Some phases are
 * meant to be run on one node only, other on all nodes.
 *
 * It is responsibility of {@link com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy}
 * to figure out when a phase should be run. The most probable place to
 * decide if a phase should be run is in {@link com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy#restore(Restorer, Operation)}.
 */
public abstract class RestorationPhase {

    protected Operation<RestoreOperationRequest> operation;

    public RestorationPhase(final Operation<RestoreOperationRequest> operation) {
        this.operation = operation;
    }

    public abstract RestorationPhaseType getRestorationPhaseType();

    public abstract void execute() throws RestorationPhaseException;

    public static final class RestorationPhaseException extends Exception {

        public RestorationPhaseException(final String message) {
            super(message);
        }

        public RestorationPhaseException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public static RestorationPhaseException construct(final RestorationPhaseType phaseType) {
            return new RestorationPhaseException(format("Unable to pass %s phase.", phaseType));
        }

        public static RestorationPhaseException construct(final Throwable throwable, final RestorationPhaseType phaseType) {
            return new RestorationPhaseException(format("Unable to pass %s phase.", phaseType), throwable);
        }

        public static RestorationPhaseException construct(final Throwable throwable, final RestorationPhaseType phaseType, final String message) {
            return new RestorationPhaseException(format("Unable to pass %s phase: %s", phaseType, message), throwable);
        }
    }

    public enum RestorationPhaseType {
        INIT,
        CLUSTER_HEALTHCHECK,
        DOWNLOAD,
        TRUNCATE,
        IMPORT,
        CLEANUP,
        UNKNOWN;

        @JsonCreator
        public static RestorationPhaseType forValue(String value) {

            if (value == null || value.isEmpty()) {
                return RestorationPhaseType.UNKNOWN;
            }

            try {
                return RestorationPhaseType.valueOf(value.trim().toUpperCase());
            } catch (final IllegalArgumentException ex) {
                return RestorationPhaseType.UNKNOWN;
            }
        }

        @JsonValue
        public String toValue() {
            return this.toString();
        }
    }

    public static class RestorationPhaseTypeConverter implements CommandLine.ITypeConverter<RestorationPhaseType> {

        @Override
        public RestorationPhaseType convert(final String value) {
            return RestorationPhaseType.forValue(value);
        }
    }

    //
    // PHASES
    //

    public static class InitPhase extends RestorationPhase {

        public InitPhase(final Operation<RestoreOperationRequest> operation) {
            super(operation);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return INIT;
        }

        @Override
        public void execute() throws RestorationPhaseException {

        }
    }

    public static class ClusterHealthCheckPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(ClusterHealthCheckPhase.class);

        private final CassandraJMXService cassandraJMXService;

        public ClusterHealthCheckPhase(final CassandraJMXService cassandraJMXService,
                                       final Operation<RestoreOperationRequest> operation) {
            super(operation);
            this.cassandraJMXService = cassandraJMXService;
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLUSTER_HEALTHCHECK;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                logger.info("Checking cluster health.");

                final boolean nodeInNormalMode = new CassandraState(cassandraJMXService, "NORMAL").act();

                if (!nodeInNormalMode) {
                    throw new IllegalStateException("This node is not in NORMAL mode!");
                }

                final int downEndpoints = new FailureDetector(cassandraJMXService).act();

                if (downEndpoints != 0) {
                    throw new IllegalStateException(format("Failure detector of this node reports that %s node(s) in a cluster are down!", downEndpoints));
                }

                final boolean validClusterState = new ClusterState(cassandraJMXService).act();

                if (!validClusterState) {
                    throw new IllegalStateException("There are either joining, leaving, moving or unreachable nodes");
                }

                final Map<String, List<String>> schemaVersions = new ClusterSchemaVersions(cassandraJMXService).act();

                if (schemaVersions.size() != 1) {
                    throw new IllegalStateException(format("There are nodes with different schemas: %s", schemaVersions));
                }

                logger.info("Cluster health check was successfully completed.");
            } catch (final Exception ex) {
                logger.error("Cluster health check has failed: {}", ex.getMessage());
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }
        }
    }

    /**
     * In this phase, we just download all data from remote location to directory
     * a particular node this operation is run on can access.
     */
    public static class DownloadingPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(DownloadingPhase.class);

        private final Restorer restorer;
        private final CassandraJMXService cassandraJMXService;

        public DownloadingPhase(final CassandraJMXService cassandraJMXService,
                                final Operation<RestoreOperationRequest> operation,
                                final Restorer restorer) {
            super(operation);
            this.restorer = restorer;
            this.cassandraJMXService = cassandraJMXService;
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return DOWNLOAD;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                if (operation.request.noDownloadData) {
                    logger.info("Skipping downloading of data.");
                    return;
                }

                logger.info("Downloading phase has started.");

                createOrCleanDirectory(operation.request.importing.sourceDir);

                final String schemaVersion = new CassandraSchemaVersion(cassandraJMXService).act();

                final Path manifest = downloadManifest(operation.request, restorer, schemaVersion);

                final List<ManifestEntry> manifestEntries = getManifestEntries(operation.request, manifest);

                final ManifestEntry tokenFile = manifestEntries.stream()
                    .filter(entry -> entry.localFile.toString().endsWith("-tokens.yaml"))
                    .findFirst().orElseThrow(() -> new IllegalStateException("There is not any token file entry in the manifest!"));

                restorer.downloadManifestEntry(tokenFile);

                if (!new CassandraSameTokens(cassandraJMXService, tokenFile.localFile).act()) {
                    throw new IllegalStateException("Tokens from snapshot and tokens of this node does not match!");
                }

                restorer.downloadFiles(manifestEntries, new OperationProgressTracker(operation, manifestEntries.size()));

                logger.info("Downloading phase was successfully completed.");
            } catch (final Exception ex) {
                logger.error("Downloading phase has failed: {}", ex.getMessage());
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }
        }
    }

    /**
     * In this phase, we truncate all tables which we are going to import.
     * This phase will be executed only on one node per whole cluster restoration
     * as it does not make sense to truncate repeatedly on every node.
     */
    public static class TruncatingPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(TruncatingPhase.class);

        private final CassandraJMXService cassandraJMXService;

        public TruncatingPhase(final CassandraJMXService cassandraJMXService,
                               final Operation<RestoreOperationRequest> operation) {
            super(operation);
            this.cassandraJMXService = cassandraJMXService;
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return TRUNCATE;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                logger.info("Truncating phase has started.");

                final Map<String, String> truncateFailuresMap = new HashMap<>();

                final List<String> filteredManifest = getFilteredManifest(operation.request, resolveLocalManifestPath(operation.request));
                final DatabaseEntities databaseEntitiesFromManifest = getRestorationEntitiesFromManifest(filteredManifest);
                final List<ImportOperationRequest> importOperationRequests = buildImportRequests(operation.request, databaseEntitiesFromManifest);

                for (final ImportOperationRequest request : importOperationRequests) {
                    try {
                        new TruncateOperation(cassandraJMXService, new TruncateOperationRequest("truncate", request.keyspace, request.table)).run();
                    } catch (Exception ex) {
                        truncateFailuresMap.put(request.keyspace + "." + request.table, ex.getMessage());
                    }
                }

                if (!truncateFailuresMap.isEmpty()) {
                    throw new RestorationPhaseException(format("Some tables were unable to be truncated: %s", truncateFailuresMap.toString()));
                }

                logger.info("Truncating phase was finished successfully");
            } catch (final Exception ex) {
                logger.error("Truncating phase has failed: {}", ex.getMessage());
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }
        }
    }

    /**
     * In this phase, we are importing downloaded SSTables for a particular node.
     * This is executed per node. Under the hood, JMX method 'loadNewSSTables' on each
     * to-be-restored column family is invoked. This works only for Cassandra 4.x clusters
     * as respective JMX method was introduced there for the first time.
     */
    public static class ImportingPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(ImportingPhase.class);

        private final CassandraJMXService cassandraJMXService;
        private final CassandraVersion cassandraVersion;

        public ImportingPhase(final CassandraJMXService cassandraJMXService,
                              final Operation<RestoreOperationRequest> operation,
                              final CassandraVersion cassandraVersion) {
            super(operation);
            this.cassandraJMXService = cassandraJMXService;
            this.cassandraVersion = cassandraVersion;
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return IMPORT;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                logger.info("Importing phase has started.");

                final List<String> filteredManifest = getFilteredManifest(operation.request, resolveLocalManifestPath(operation.request));
                final DatabaseEntities databaseEntitiesFromManifest = getRestorationEntitiesFromManifest(filteredManifest);
                final List<ImportOperationRequest> importOperationRequests = buildImportRequests(operation.request, databaseEntitiesFromManifest);

                for (final ImportOperationRequest request : importOperationRequests) {
                    new ImportOperation(cassandraJMXService, cassandraVersion, request).run();
                }

                logger.info("Importing phase was finished successfully.");
            } catch (final Exception ex) {
                logger.error("Importing phase has failed: {}", ex.getMessage());
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }
        }
    }

    /**
     * In this phase, we are importing downloaded SSTables by making hardlinks from download dir to directory of SSTables.
     * Once this is done, we refresh a node so they become active.
     */
    public static class HardlinkingPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(HardlinkingPhase.class);

        private final CassandraJMXService cassandraJMXService;

        public HardlinkingPhase(final CassandraJMXService cassandraJMXService,
                                final Operation<RestoreOperationRequest> operation) {
            super(operation);
            this.cassandraJMXService = cassandraJMXService;
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return IMPORT;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                logger.info("Hardlinking phase has started.");

                final List<String> filteredManifest = getFilteredManifest(operation.request, resolveLocalManifestPath(operation.request));
                final DatabaseEntities databaseEntities = getRestorationEntitiesFromManifest(filteredManifest);

                final List<ManifestEntry> manifestEntries = getManifestEntriesWithoutSchemaCqls(operation.request);

                // make links

                for (final ManifestEntry entry : manifestEntries) {
                    final Path link = operation.request.cassandraDirectory.resolve(operation.request.importing.sourceDir.relativize(entry.localFile));
                    final Path existing = entry.localFile;

                    if (existing.toFile().toString().endsWith("-tokens.yaml")) {
                        FileUtils.createDirectory(link.getParent());
                        Files.copy(existing, link, REPLACE_EXISTING);
                    } else {
                        try {
                            Files.createLink(link, existing);
                        } catch (final Exception ex) {
                            throw new IllegalStateException(format("Unable to create a hardlink from %s to %s",
                                                                   existing.toAbsolutePath().toString(),
                                                                   link.toAbsolutePath().toString()),
                                                            ex);
                        }
                    }
                }

                // refresh

                final Map<String, String> failedRefreshes = new HashMap<>();

                for (final Entry<String, String> entry : databaseEntities.getKeyspacesAndTables().entries()) {
                    try {
                        new RefreshOperation(cassandraJMXService, new RefreshOperationRequest(entry.getKey(), entry.getValue())).run();
                    } catch (final Exception ex) {
                        failedRefreshes.put(entry.getKey() + "." + entry.getValue(), ex.getMessage());
                    }
                }

                if (!failedRefreshes.isEmpty()) {
                    throw new RestorationPhaseException(format("Failed tables to refresh: %s", failedRefreshes));
                }

                logger.info("Hardlinking phase was finished successfully.");
            } catch (final Exception ex) {
                logger.error("Hardlinking phase has failed: {}", ex.getMessage());
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }
        }
    }

    public static class CleaningPhase extends RestorationPhase {

        public CleaningPhase(final Operation<RestoreOperationRequest> operation) {
            super(operation);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLEANUP;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            if (!operation.request.noDeleteTruncates) {
                new TruncateDirCleaningPhase(operation).execute();
            }

            if (!operation.request.noDeleteDownloads) {
                new DownloadDirCleaningPhase(operation).execute();
            }
        }
    }

    public static class DownloadDirCleaningPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(DownloadDirCleaningPhase.class);

        public DownloadDirCleaningPhase(final Operation<RestoreOperationRequest> operation) {
            super(operation);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLEANUP;
        }

        @Override
        public void execute() throws RestorationPhaseException {

            if (operation.request.noDeleteDownloads) {
                logger.info("Skipping deletion of downloaded data dirs.");
                return;
            }

            try {
                final Path sourceDir = operation.request.importing.sourceDir;
                logger.info("Deleting {}", sourceDir.toAbsolutePath().toString());
                FileUtils.deleteDirectory(operation.request.importing.sourceDir);
            } catch (final Exception ex) {
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }
        }
    }

    public static class TruncateDirCleaningPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(TruncateDirCleaningPhase.class);

        public TruncateDirCleaningPhase(final Operation<RestoreOperationRequest> operation) {
            super(operation);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLEANUP;
        }

        @Override
        public void execute() throws RestorationPhaseException {

            if (operation.request.noDeleteTruncates) {
                logger.info("Skipping deletion of truncated directories.");
                return;
            }

            final File cassandraDataDir = operation.request.cassandraDirectory.resolve("data").toFile();

            final Collection<File> truncateDirs = listFilesAndDirs(cassandraDataDir,
                                                                   new FileFileFilter() {
                                                                       @Override
                                                                       public boolean accept(final File file) {
                                                                           return false;
                                                                       }
                                                                   },
                                                                   new DirectoryFileFilter() {
                                                                       @Override
                                                                       public boolean accept(final File file) {
                                                                           return file.getName().startsWith("truncated");
                                                                       }
                                                                   });

            truncateDirs.remove(cassandraDataDir);

            // double check
            for (final File truncateDir : truncateDirs) {
                if (!truncateDir.getName().startsWith("truncated")) {
                    throw new IllegalStateException(format("A truncated directory to delete do not start on \"truncated\": %s",
                                                           truncateDir.getAbsolutePath()));
                }
                logger.info("Going to delete directory {}", truncateDir.getAbsolutePath());
            }

            for (final File truncateDir : truncateDirs) {
                try {
                    logger.info("Deleting {}", truncateDir.getAbsolutePath());
                    FileUtils.deleteDirectory(truncateDir);
                } catch (final Exception ex) {
                    logger.info("Deleting truncated directories has failed: {}", ex.getMessage());
                    throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
                }
            }

            logger.info("Deleting truncated directories has finished successfully.");
        }
    }
}
