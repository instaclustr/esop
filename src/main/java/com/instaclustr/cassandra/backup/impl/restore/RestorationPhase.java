package com.instaclustr.cassandra.backup.impl.restore;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.CLUSTER_HEALTHCHECK;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.INIT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.buildImportRequests;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.downloadManifest;
import static com.instaclustr.io.FileUtils.createOrCleanDirectory;
import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.listFilesAndDirs;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.instaclustr.cassandra.backup.impl.AbstractTracker.Session;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.Manifest;
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
import com.instaclustr.cassandra.backup.impl.restore.DownloadTracker.DownloadUnit;
import com.instaclustr.cassandra.backup.impl.restore.strategy.RestorationContext;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperation;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperationRequest;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.Operation;
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

    protected RestorationContext ctxt;

    public RestorationPhase(RestorationContext ctxt) {
        this.ctxt = ctxt;
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

        public InitPhase(final RestorationContext ctxt) {
            super(ctxt);
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

        public ClusterHealthCheckPhase(final RestorationContext ctxt) {
            super(ctxt);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLUSTER_HEALTHCHECK;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                logger.info("Checking cluster health.");

                final boolean nodeInNormalMode = new CassandraState(ctxt.jmx, "NORMAL").act();

                if (!nodeInNormalMode) {
                    throw new IllegalStateException("This node is not in NORMAL mode!");
                }

                final int downEndpoints = new FailureDetector(ctxt.jmx).act();

                if (downEndpoints != 0) {
                    throw new IllegalStateException(format("Failure detector of this node reports that %s node(s) in a cluster are down!", downEndpoints));
                }

                final boolean validClusterState = new ClusterState(ctxt.jmx).act();

                if (!validClusterState) {
                    throw new IllegalStateException("There are either joining, leaving, moving or unreachable nodes");
                }

                final Map<String, List<String>> schemaVersions = new ClusterSchemaVersions(ctxt.jmx).act();

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


        public DownloadingPhase(final RestorationContext ctxt) {
            super(ctxt);

        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return DOWNLOAD;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                if (ctxt.operation.request.noDownloadData) {
                    logger.info("Skipping downloading of data.");
                    return;
                }

                logger.info("Downloading phase has started.");

                createOrCleanDirectory(ctxt.operation.request.importing.sourceDir);

                final String schemaVersion = new CassandraSchemaVersion(ctxt.jmx).act();

                final Manifest manifest = downloadManifest(ctxt.operation.request, ctxt.restorer, schemaVersion, ctxt.objectMapper);
                manifest.enrichManifestEntries(ctxt.operation.request.importing.sourceDir);

                new CassandraSameTokens(ctxt.jmx, manifest.getTokens()).act();

                // looking into downloaded manifest, download only these sstables for keyspaces / tables
                // which were specified in request in "entities"
                final List<ManifestEntry> manifestFiles = manifest.getManifestFiles(ctxt.operation.request.entities,
                                                                                    false,  // not possible to restore system keyspace on a live cluster
                                                                                    false); // no new cluster

                Session<DownloadUnit> session = null;

                try {
                    session = ctxt.downloadTracker.submit(ctxt.restorer,
                                                          ctxt.operation,
                                                          manifestFiles,
                                                          ctxt.operation.request.snapshotTag,
                                                          ctxt.operation.request.concurrentConnections);

                    session.waitUntilConsideredFinished();
                    ctxt.downloadTracker.cancelIfNecessary(session);
                } finally {
                    ctxt.downloadTracker.removeSession(session);
                }

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

        public TruncatingPhase(final RestorationContext ctxt) {
            super(ctxt);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return TRUNCATE;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                logger.info("Truncating phase has started.");

                final String schemaVersion = new CassandraSchemaVersion(ctxt.jmx).act();
                final Manifest manifest = downloadManifest(ctxt.operation.request, ctxt.restorer, schemaVersion, ctxt.objectMapper);
                manifest.enrichManifestEntries(ctxt.operation.request.importing.sourceDir);
                final DatabaseEntities filteredEntities = manifest.getDatabaseEntities(false).filter(ctxt.operation.request.entities, false);
                final List<ImportOperationRequest> importOperationRequests = buildImportRequests(ctxt.operation.request, filteredEntities);

                final Map<String, String> truncateFailuresMap = new HashMap<>();

                for (final ImportOperationRequest request : importOperationRequests) {
                    try {
                        new TruncateOperation(ctxt.jmx, new TruncateOperationRequest("truncate", request.keyspace, request.table)).run();
                    } catch (Exception ex) {
                        truncateFailuresMap.put(format("%s.%s", request.keyspace, request.table), ex.getMessage());
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

        public ImportingPhase(final RestorationContext ctxt) {
            super(ctxt);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return IMPORT;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                logger.info("Importing phase has started.");

                final String schemaVersion = new CassandraSchemaVersion(ctxt.jmx).act();
                final Manifest manifest = downloadManifest(ctxt.operation.request, ctxt.restorer, schemaVersion, ctxt.objectMapper);
                manifest.enrichManifestEntries(ctxt.operation.request.importing.sourceDir);
                final DatabaseEntities filteredEntities = manifest.getDatabaseEntities(false).filter(ctxt.operation.request.entities, false);
                final List<ImportOperationRequest> importOperationRequests = buildImportRequests(ctxt.operation.request, filteredEntities);

                for (final ImportOperationRequest request : importOperationRequests) {
                    new ImportOperation(ctxt.jmx, ctxt.cassandraVersion, request).run();
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

        public HardlinkingPhase(final RestorationContext ctxt) {
            super(ctxt);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return IMPORT;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            try {
                logger.info("Hardlinking phase has started.");

                final String schemaVersion = new CassandraSchemaVersion(ctxt.jmx).act();
                final Manifest manifest = downloadManifest(ctxt.operation.request, ctxt.restorer, schemaVersion, ctxt.objectMapper);
                manifest.enrichManifestEntries(ctxt.operation.request.importing.sourceDir);
                final DatabaseEntities filteredEntities = manifest.getDatabaseEntities(false).filter(ctxt.operation.request.entities, false);

                final List<ManifestEntry> manifestEntries = manifest.getManifestFiles(filteredEntities,
                                                                                      false /* not possible to restore system keyspace on a live cluster */,
                                                                                      false); // without schema.cql's

                // make links

                boolean failedLinkage = false;
                final List<Path> successfulLinks = new ArrayList<>();

                for (final ManifestEntry entry : manifestEntries) {

                    if (failedLinkage) {
                        break;
                    }

                    final Path link = ctxt.operation.request.cassandraDirectory.resolve(ctxt.operation.request.importing.sourceDir.relativize(entry.localFile));
                    final Path existing = entry.localFile;

                    try {
                        Files.createLink(link, existing);
                        successfulLinks.add(link);
                    } catch (final Exception ex) {
                        logger.error(format("Unable to create a hardlink from %s to %s, skipping the linking of all other resources and deleting already linked ones.",
                                            existing.toAbsolutePath().toString(),
                                            link.toAbsolutePath().toString()),
                                     ex);

                        failedLinkage = true;
                    }
                }

                if (failedLinkage && !successfulLinks.isEmpty()) {
                    for (final Path linked : successfulLinks) {
                        try {
                            Files.deleteIfExists(linked);
                        } catch (final Exception ex) {
                            logger.error(format("It is not possible to delete link %s.", linked.toString()), ex);
                        }
                    }

                    throw new RestorationPhaseException("Hardlinking phase finished with errors, the linking of downloaded SSTables to Cassandra directory has failed.");
                } else {
                    final Map<String, String> failedRefreshes = new HashMap<>();

                    for (final Entry<String, String> entry : filteredEntities.getKeyspacesAndTables().entries()) {
                        try {
                            new RefreshOperation(ctxt.jmx, new RefreshOperationRequest(entry.getKey(), entry.getValue())).run();
                        } catch (final Exception ex) {
                            failedRefreshes.put(entry.getKey() + "." + entry.getValue(), ex.getMessage());
                        }
                    }

                    if (!failedRefreshes.isEmpty()) {
                        throw new RestorationPhaseException(format("Failed tables to refresh: %s", failedRefreshes));
                    }

                    logger.info("Hardlinking phase was finished successfully.");
                }
            } catch (
                final Exception ex) {
                logger.error("Hardlinking phase has failed: {}", ex.getMessage());
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }
        }
    }

    public static class CleaningPhase extends RestorationPhase {

        public CleaningPhase(final RestorationContext ctxt) {
            super(ctxt);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLEANUP;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            if (!ctxt.operation.request.noDeleteTruncates) {
                new TruncateDirCleaningPhase(ctxt).execute();
            }

            if (!ctxt.operation.request.noDeleteDownloads) {
                new DownloadDirCleaningPhase(ctxt).execute();
            }
        }
    }

    public static class DownloadDirCleaningPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(DownloadDirCleaningPhase.class);

        public DownloadDirCleaningPhase(final RestorationContext ctxt) {
            super(ctxt);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLEANUP;
        }

        @Override
        public void execute() throws RestorationPhaseException {

            if (ctxt.operation.request.noDeleteDownloads) {
                logger.info("Skipping deletion of downloaded data dirs.");
                return;
            }

            try {
                final Path sourceDir = ctxt.operation.request.importing.sourceDir;
                logger.info("Deleting {}", sourceDir.toAbsolutePath().toString());
                FileUtils.deleteDirectory(ctxt.operation.request.importing.sourceDir);
            } catch (final Exception ex) {
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }
        }
    }

    public static class TruncateDirCleaningPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(TruncateDirCleaningPhase.class);

        public TruncateDirCleaningPhase(final RestorationContext ctxt) {
            super(ctxt);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLEANUP;
        }

        @Override
        public void execute() throws RestorationPhaseException {

            if (ctxt.operation.request.noDeleteTruncates) {
                logger.info("Skipping deletion of truncated directories.");
                return;
            }

            final File cassandraDataDir = ctxt.operation.request.cassandraDirectory.resolve("data").toFile();

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
