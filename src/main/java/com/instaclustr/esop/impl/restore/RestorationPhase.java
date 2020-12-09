package com.instaclustr.esop.impl.restore;

import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.CLUSTER_HEALTHCHECK;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.INIT;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static com.instaclustr.io.FileUtils.createOrCleanDirectory;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.instaclustr.esop.ManifestEnricher;
import com.instaclustr.esop.impl.AbstractTracker.Session;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.CassandraData;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl._import.ImportOperation;
import com.instaclustr.esop.impl._import.ImportOperationRequest;
import com.instaclustr.esop.impl.interaction.CassandraSameTokens;
import com.instaclustr.esop.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.esop.impl.interaction.CassandraState;
import com.instaclustr.esop.impl.interaction.ClusterSchemaVersions;
import com.instaclustr.esop.impl.interaction.ClusterState;
import com.instaclustr.esop.impl.interaction.FailureDetector;
import com.instaclustr.esop.impl.refresh.RefreshOperation;
import com.instaclustr.esop.impl.refresh.RefreshOperationRequest;
import com.instaclustr.esop.impl.restore.DownloadTracker.DownloadUnit;
import com.instaclustr.esop.impl.restore.strategy.RestorationContext;
import com.instaclustr.esop.impl.truncate.TruncateOperation;
import com.instaclustr.esop.impl.truncate.TruncateOperationRequest;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Phase of a cluster-wide restoration. Some phases are
 * meant to be run on one node only, other on all nodes.
 *
 * It is responsibility of {@link RestorationStrategy}
 * to figure out when a phase should be run. The most probable place to
 * decide if a phase should be run is in {@link RestorationStrategy#restore(Restorer, Operation)}.
 */
public abstract class RestorationPhase {

    protected RestorationContext ctxt;

    public RestorationPhase(RestorationContext ctxt, boolean parseCassandraData) throws Exception {
        this.ctxt = ctxt;

        if (parseCassandraData) {
            final CassandraData cassandraData = CassandraData.parse(ctxt.operation.request.cassandraDirectory.resolve("data"));
            cassandraData.setDatabaseEntitiesFromRequest(ctxt.operation.request.entities);
            cassandraData.setRenamedEntitiesFromRequest(ctxt.operation.request.rename);
            this.ctxt.cassandraData = cassandraData;
        }
    }

    public RestorationPhase(RestorationContext ctxt) throws Exception {
        this(ctxt, false);
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

        public InitPhase(final RestorationContext ctxt) throws Exception {
            super(ctxt, false);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return INIT;
        }

        @Override
        public void execute() {
        }
    }

    public static class ClusterHealthCheckPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(ClusterHealthCheckPhase.class);

        public ClusterHealthCheckPhase(final RestorationContext ctxt) throws Exception {
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

        public DownloadingPhase(final RestorationContext ctxt) throws Exception {
            super(ctxt, true);
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

                final RestoreOperationRequest request = ctxt.operation.request;

                if (!ctxt.operation.request.skipBucketVerification) {
                    try (final BucketService bucketService = ctxt.bucketServiceFactoryMap.get(request.storageLocation.storageProvider).createBucketService(request)) {
                        bucketService.checkBucket(request.storageLocation.bucket, false);
                    }
                }

                final String schemaVersion = new CassandraSchemaVersion(ctxt.jmx).act();

                final Manifest manifest = RestorationUtilities.downloadManifest(request, ctxt.restorer, schemaVersion, ctxt.objectMapper);

                // verify that we are downloading data for same token so data fit a node
                new CassandraSameTokens(ctxt.jmx, manifest.getTokens()).act();

                createOrCleanDirectory(ctxt.operation.request.importing.sourceDir);

                new ManifestEnricher().enrich(ctxt.cassandraData, manifest, ctxt.operation.request.importing.sourceDir);

                // looking into downloaded manifest, download only these sstables for keyspaces / tables
                // which were specified in request in "entities"
                // there will be only entries in this list which are backed by existing keyspace on disk
                final List<ManifestEntry> manifestFiles = manifest.getManifestFiles(request.entities,
                                                                                    false,  // not possible to restore system keyspace on a live cluster
                                                                                    false,  // no new cluster
                                                                                    false); // with schemas

                Session<DownloadUnit> session = null;

                try {
                    session = ctxt.downloadTracker.submit(ctxt.restorer,
                                                          ctxt.operation,
                                                          manifestFiles,
                                                          request.snapshotTag,
                                                          request.concurrentConnections);

                    session.waitUntilConsideredFinished();
                    ctxt.downloadTracker.cancelIfNecessary(session);
                } finally {
                    ctxt.downloadTracker.removeSession(session);
                    session = null;
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

        public TruncatingPhase(final RestorationContext ctxt) throws Exception {
            super(ctxt, true);
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
                final Manifest manifest = RestorationUtilities.downloadManifest(ctxt.operation.request,
                                                                                ctxt.restorer,
                                                                                schemaVersion,
                                                                                ctxt.objectMapper);

                new ManifestEnricher().enrich(ctxt.cassandraData, manifest, ctxt.operation.request.importing.sourceDir);

                final DatabaseEntities toTruncate = ctxt.cassandraData.getDatabaseEntitiesToProcessForRestore(manifest);

                if (toTruncate.getKeyspacesAndTables().isEmpty()) {
                    logger.info("It is not necessary to truncate any table.");
                    return;
                } else {
                    logger.info(String.format("Going to truncate these tables: %s", toTruncate.getKeyspacesAndTables().toString()));
                }

                final Map<String, String> truncateFailuresMap = new HashMap<>();

                for (final Map.Entry<String, String> request : toTruncate.getKeyspacesAndTables().entries()) {
                    try {
                        new TruncateOperation(ctxt.jmx, new TruncateOperationRequest(request.getKey(), request.getValue())).run();
                    } catch (Exception ex) {
                        truncateFailuresMap.put(format("%s.%s", request.getKey(), request.getValue()), ex.getMessage());
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

        public ImportingPhase(final RestorationContext ctxt) throws Exception {
            super(ctxt, true);
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
                final Manifest manifest = RestorationUtilities.downloadManifest(ctxt.operation.request, ctxt.restorer, schemaVersion, ctxt.objectMapper);
                new ManifestEnricher().enrich(ctxt.cassandraData, manifest, ctxt.operation.request.importing.sourceDir);
                final DatabaseEntities databaseEntitiesToProcess = ctxt.cassandraData.getDatabaseEntitiesToProcessForRestore(manifest);

                final List<ImportOperationRequest> imports = databaseEntitiesToProcess
                    .getKeyspacesAndTables()
                    .entries()
                    .stream()
                    // it has table in cassandra
                    .filter(entry -> ctxt.cassandraData.getTablePath(entry.getKey(), entry.getValue()).isPresent())
                    .filter(entry -> ctxt.cassandraData.getTableId(entry.getKey(), entry.getValue()).isPresent())
                    .map(entry -> {
                        final String keyspace = entry.getKey();
                        final String table = entry.getValue();
                        final String tableWithId = format("%s-%s", table, ctxt.cassandraData.getTableId(entry.getKey(), entry.getValue()).get());
                        final Path tablePath = ctxt.operation.request.importing.sourceDir.resolve(keyspace).resolve(tableWithId);
                        return ctxt.operation.request.importing.copy(keyspace, table, tablePath);
                    })
                    .filter(request -> Files.isDirectory(request.tablePath)).collect(toList());

                for (final ImportOperationRequest request : imports) {
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

        public HardlinkingPhase(final RestorationContext ctxt) throws Exception {
            super(ctxt, true);
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
                final Manifest manifest = RestorationUtilities.downloadManifest(ctxt.operation.request, ctxt.restorer, schemaVersion, ctxt.objectMapper);
                new ManifestEnricher().enrich(ctxt.cassandraData, manifest, ctxt.operation.request.importing.sourceDir);
                final DatabaseEntities databaseEntitiesToProcess = ctxt.cassandraData.getDatabaseEntitiesToProcessForRestore(manifest);

                final List<Path> downloadedFiles = CassandraData.list(ctxt.operation.request.importing.sourceDir);

                // make links

                boolean failedLinkage = false;
                final List<Path> successfulLinks = new ArrayList<>();

                for (final Path existing : downloadedFiles) {

                    if (failedLinkage) {
                        break;
                    }

                    final Path link = ctxt.operation.request.cassandraDirectory.resolve("data").resolve(ctxt.operation.request.importing.sourceDir.relativize(existing));

                    try {
                        logger.debug(String.format("linking from %s to %s", existing, link));
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

                    for (final Entry<String, String> entry : databaseEntitiesToProcess.getKeyspacesAndTables().entries()) {
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

        public CleaningPhase(final RestorationContext ctxt) throws Exception {
            super(ctxt, false);
        }

        @Override
        public RestorationPhaseType getRestorationPhaseType() {
            return CLEANUP;
        }

        @Override
        public void execute() throws RestorationPhaseException {
            if (!ctxt.operation.request.noDeleteTruncates) {
                try {
                    new TruncateDirCleaningPhase(ctxt).execute();
                } catch (final RestorationPhaseException ex) {
                    throw ex;
                } catch (final Exception ex) {
                    throw new RestorationPhaseException("Unable to clean trucate dirs!", ex);
                }
            }

            if (!ctxt.operation.request.noDeleteDownloads) {
                try {
                    new DownloadDirCleaningPhase(ctxt).execute();
                } catch (final RestorationPhaseException ex) {
                    throw ex;
                } catch (final Exception ex) {
                    throw new RestorationPhaseException("Unable to clean download dir!", ex);
                }
            }
        }
    }

    public static class DownloadDirCleaningPhase extends RestorationPhase {

        private static final Logger logger = LoggerFactory.getLogger(DownloadDirCleaningPhase.class);

        public DownloadDirCleaningPhase(final RestorationContext ctxt) throws Exception {
            super(ctxt, false);
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

        public TruncateDirCleaningPhase(final RestorationContext ctxt) throws Exception {
            super(ctxt, false);
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

            final Path cassandraDataDir = ctxt.operation.request.cassandraDirectory.resolve("data");

            try {
                final List<Path> truncateDirs = CassandraData.listDirs(cassandraDataDir, new Predicate<Path>() {
                    @Override
                    public boolean test(final Path dir) {
                        return dir.getFileName().toString().startsWith("truncated-")
                            && dir.getParent().getFileName().toString().equals("snapshots");
                    }
                });

                for (final Path truncateDir : truncateDirs) {
                    logger.info("Deleting {}", truncateDir.toAbsolutePath().toString());
                    FileUtils.deleteDirectory(truncateDir);
                }
            } catch (final Exception ex) {
                logger.info("Deleting truncated directories has failed: {}", ex.getMessage());
                throw RestorationPhaseException.construct(ex, getRestorationPhaseType());
            }

            logger.info("Deleting truncated directories has finished successfully.");
        }
    }
}
