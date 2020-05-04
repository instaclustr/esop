package com.instaclustr.cassandra.backup.impl.backup.coordination;

import static com.instaclustr.cassandra.backup.impl.ManifestEntry.Type.FILE;
import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.instaclustr.cassandra.backup.guice.BackuperFactory;
import com.instaclustr.cassandra.backup.guice.BucketServiceFactory;
import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl.ManifestEntry.Type;
import com.instaclustr.cassandra.backup.impl.SSTableUtils;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupPhaseResultGatherer;
import com.instaclustr.cassandra.backup.impl.backup.Backuper;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraTokens;
import com.instaclustr.io.FileUtils;
import com.instaclustr.io.GlobalLock;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.OperationProgressTracker;
import com.instaclustr.operations.OperationRequest;
import com.instaclustr.operations.ResultGatherer;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseBackupOperationCoordinator extends OperationCoordinator<BackupOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(BaseBackupOperationCoordinator.class);

    protected final CassandraJMXService cassandraJMXService;
    protected final Map<String, BackuperFactory> backuperFactoryMap;
    protected final Map<String, BucketServiceFactory> bucketServiceFactoryMap;

    public BaseBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                          final Map<String, BackuperFactory> backuperFactoryMap,
                                          final Map<String, BucketServiceFactory> bucketServiceFactoryMap) {
        this.cassandraJMXService = cassandraJMXService;
        this.backuperFactoryMap = backuperFactoryMap;
        this.bucketServiceFactoryMap = bucketServiceFactoryMap;
    }

    @Override
    public ResultGatherer<BackupOperationRequest> coordinate(final Operation<BackupOperationRequest> operation) throws OperationCoordinatorException {

        final BackupOperationRequest request = operation.request;

        logger.info(request.toString());

        FileLock fileLock = null;

        final BackupPhaseResultGatherer gatherer = new BackupPhaseResultGatherer();

        Throwable cause = null;

        try {
            assert cassandraJMXService != null;
            assert backuperFactoryMap != null;
            assert bucketServiceFactoryMap != null;

            fileLock = new GlobalLock(request.lockFile).waitForLock();

            new TakeSnapshotOperation(cassandraJMXService, new TakeSnapshotOperationRequest(request.entities, request.snapshotTag)).run0();

            final List<String> tokens = new CassandraTokens(cassandraJMXService).act();

            logger.info("Tokens " + tokens);

            final String schemaVersion = new CassandraSchemaVersion(cassandraJMXService).act();

            logger.info("Schema version " + schemaVersion);

            final UploadingHelper holder = new UploadingHelper();

            holder.manifestEntries = generateManifest(request.snapshotTag, request.cassandraDirectory.resolve("data"));

            // token list

            Path localTokenListPath = UploadingHelper.getLocalTokenListPath(request, schemaVersion);
            UploadingHelper.writeTokenListLocally(tokens, localTokenListPath);
            ManifestEntry tokenListAsManifestEntry = UploadingHelper.getTokenListAsManifestEntry(localTokenListPath);
            holder.tokens = tokenListAsManifestEntry;
            holder.addTokenListIntoManifestEntries(tokenListAsManifestEntry);

            // manifest

            Path localManifestPath = UploadingHelper.getLocalManifestPath(request, schemaVersion);
            UploadingHelper.writeManifestFileLocally(localManifestPath, holder.manifestEntries);
            ManifestEntry manifestAsManifestEntry = UploadingHelper.getManifestAsManifestEntry(localManifestPath);
            holder.manifest = manifestAsManifestEntry;
            holder.addManifestIntoManifestEntries(manifestAsManifestEntry);

            BucketService bucketService = null;
            Backuper backuper = null;

            try {
                bucketService = bucketServiceFactoryMap.get(request.storageLocation.storageProvider).createBucketService(request);

                bucketService.createIfMissing(request.storageLocation.bucket);

                backuper = backuperFactoryMap.get(request.storageLocation.storageProvider).createBackuper(request);

                backuper.uploadOrFreshenFiles(operation, holder.manifestEntries, new OperationProgressTracker(operation, holder.manifestEntries.size()));
            } finally {
                if (bucketService != null) {
                    bucketService.close();
                }

                if (backuper != null) {
                    backuper.close();
                }

                UploadingHelper.cleanup(holder);
            }
        } catch (final Exception ex) {
            logger.error("Unable to perform a backup!", ex);
            cause = ex;
        } finally {
            try {
                new ClearSnapshotOperation(cassandraJMXService, new ClearSnapshotOperationRequest(request.snapshotTag)).run0();
                logger.info("Snapshot '{}' cleared", request.snapshotTag);
            } catch (final Exception ex) {
                logger.error(String.format("Unable to clear snapshot '%s' after backup!", request.snapshotTag), ex);
                if (cause == null) {
                    cause = ex;
                }
            }

            try {
                if (fileLock != null) {
                    fileLock.release();
                }
            } catch (final Exception ex) {
                if (cause != null) {
                    cause = new OperationCoordinatorException(format("Unable to release file lock on a backup %s", operation), ex);
                }
            }
        }

        gatherer.gather(operation, cause);

        return gatherer;
    }


    private Collection<ManifestEntry> generateManifest(final String snapshotTag, final Path cassandraDataDirectory) throws IOException {

        // find files belonging to snapshot
        final List<KeyspaceColumnFamilySnapshot> kcfss = findSnapshots(cassandraDataDirectory, snapshotTag);

        if (kcfss.isEmpty()) {
            throw new IllegalStateException(format("There are not any SSTables belonging to snapshot %s", snapshotTag));
        }

        // generate manifest (set of object keys and source files defining the snapshot)
        final Collection<ManifestEntry> manifest = new LinkedList<>(); // linked list to maintain order

        // add snapshot files to the manifest
        for (final KeyspaceColumnFamilySnapshot kscf : kcfss) {
            final Path tablePath = Paths.get("data").resolve(Paths.get(kscf.keyspace, kscf.table));
            Iterables.addAll(manifest, SSTableUtils.ssTableManifest(kscf.snapshotDirectory, tablePath).collect(toList()));

            Path schemaPath = kscf.snapshotDirectory.resolve("schema.cql");

            if (Files.exists(schemaPath)) {
                manifest.add(new ManifestEntry(tablePath.resolve(snapshotTag + "-schema.cql"), schemaPath, FILE));
            }
        }

        logger.info("{} files in manifest for snapshot {}.", manifest.size(), snapshotTag);

        return manifest;
    }

    private static class UploadingHelper {

        public Collection<ManifestEntry> manifestEntries;

        public ManifestEntry tokens;

        public ManifestEntry manifest;

        // token list

        public static void cleanup(final UploadingHelper uploadingHelper) throws Exception {
            Files.deleteIfExists(uploadingHelper.tokens.localFile);
            Files.deleteIfExists(uploadingHelper.manifest.localFile);
        }

        public static void writeTokenListLocally(List<String> tokens, final Path localTokenListFile) throws Exception {
            FileUtils.createFile(localTokenListFile);
            try (final OutputStream stream = Files.newOutputStream(localTokenListFile);
                final PrintStream writer = new PrintStream(stream)) {
                writer.println("# automatically generated by cassandra-backup");
                writer.println("# add the following to cassandra.yaml when restoring to a new cluster.");
                writer.printf("initial_token: %s%n", Joiner.on(',').join(tokens));
            }
        }

        public void addTokenListIntoManifestEntries(final ManifestEntry tokenList) {
            this.manifestEntries.add(tokenList);
        }

        public static Path getLocalTokenListPath(final BackupOperationRequest request, final String schemaVersion) {
            return request.cassandraDirectory.resolve("data/tokens").resolve(format("%s-%s-tokens.yaml", request.snapshotTag, schemaVersion));
        }

        public static ManifestEntry getTokenListAsManifestEntry(final Path localTokenListPath) throws Exception {
            return new ManifestEntry(Paths.get("tokens").resolve(localTokenListPath.getFileName()), localTokenListPath, FILE);
        }

        // manifest file

        public static void writeManifestFileLocally(final Path localManifestPath, Collection<ManifestEntry> manifestEntries) throws Exception {
            FileUtils.createFile(localManifestPath);
            try (final OutputStream stream = Files.newOutputStream(localManifestPath);
                final PrintStream writer = new PrintStream(stream)) {
                for (final ManifestEntry manifestEntry : manifestEntries) {
                    writer.println(Joiner.on(' ').join(manifestEntry.size, manifestEntry.objectKey));
                }
            }
        }

        public void addManifestIntoManifestEntries(final ManifestEntry manifest) {
            this.manifestEntries.add(manifest);
        }

        public static ManifestEntry getManifestAsManifestEntry(final Path localManifestPath) throws Exception {
            return new ManifestEntry(Paths.get("manifests").resolve(localManifestPath.getFileName()), localManifestPath, ManifestEntry.Type.MANIFEST_FILE);
        }

        public static Path getLocalManifestPath(final BackupOperationRequest request, final String schemaVersion) {
            return request.cassandraDirectory.resolve("manifests").resolve(request.snapshotTag + "-" + schemaVersion);
        }
    }

    private static List<KeyspaceColumnFamilySnapshot> findSnapshots(final Path cassandraDataDirectory, final String snapshotTag) throws IOException {
        // /var/lib/cassandra/data/<keyspace>/<table>/snapshots/<snapshot>
        return Files.find(cassandraDataDirectory,
                          4,
                          (path, basicFileAttributes) -> basicFileAttributes.isDirectory() && path.getParent().endsWith("snapshots"))
            .map((KeyspaceColumnFamilySnapshot::new))
            .collect(groupingBy(k -> k.snapshotDirectory.getFileName().toString()))
            .getOrDefault(snapshotTag, new ArrayList<>());
    }

    private static class KeyspaceColumnFamilySnapshot {

        final String keyspace, table;
        final Path snapshotDirectory;

        KeyspaceColumnFamilySnapshot(final Path snapshotDirectory) {
            // ...data/<keyspace>/<table>/snapshots/<snapshotDirectory>

            final Path columnFamilyDirectory = snapshotDirectory.getParent().getParent();

            this.table = columnFamilyDirectory.getFileName().toString();
            this.keyspace = columnFamilyDirectory.getParent().getFileName().toString();
            this.snapshotDirectory = snapshotDirectory;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("keyspace", keyspace)
                .add("table", table)
                .add("snapshotDirectory", snapshotDirectory)
                .toString();
        }
    }

    private static class ClearSnapshotOperation extends Operation<ClearSnapshotOperationRequest> {

        private static final Logger logger = LoggerFactory.getLogger(ClearSnapshotOperation.class);

        private final CassandraJMXService cassandraJMXService;
        private boolean hasRun = false;

        public ClearSnapshotOperation(final CassandraJMXService cassandraJMXService,
                                      final ClearSnapshotOperationRequest request) {
            super(request);
            this.cassandraJMXService = cassandraJMXService;
        }

        @Override
        protected void run0() {
            if (hasRun) {
                return;
            }

            hasRun = true;

            try {
                cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Void>() {
                    @Override
                    public Void apply(StorageServiceMBean ssMBean) throws Exception {
                        ssMBean.clearSnapshot(request.snapshotTag);
                        return null;
                    }
                });

                logger.info("Cleared snapshot {}.", request.snapshotTag);
            } catch (final Exception ex) {
                logger.error("Failed to cleanup snapshot {}.", request.snapshotTag, ex);
            }
        }
    }

    private static class ClearSnapshotOperationRequest extends OperationRequest {

        final String snapshotTag;

        ClearSnapshotOperationRequest(final String snapshotTag) {
            this.snapshotTag = snapshotTag;
        }
    }

    private static class TakeSnapshotOperation extends Operation<TakeSnapshotOperationRequest> {

        private static final Logger logger = LoggerFactory.getLogger(TakeSnapshotOperation.class);

        private final TakeSnapshotOperationRequest request;
        private final CassandraJMXService cassandraJMXService;

        public TakeSnapshotOperation(final CassandraJMXService cassandraJMXService,
                                     final TakeSnapshotOperationRequest request) {
            super(request);
            this.request = request;
            this.cassandraJMXService = cassandraJMXService;
        }

        @Override
        protected void run0() throws Exception {
            if (request.entities.areEmpty()) {
                logger.info("Taking snapshot '{}' on all keyspaces.", request.tag);
            } else {
                logger.info("Taking snapshot '{}' on {}", request.tag, request.entities);
            }

            cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Void>() {
                @Override
                public Void apply(StorageServiceMBean ssMBean) throws Exception {

                    if (request.entities.areEmpty()) {
                        ssMBean.takeSnapshot(request.tag, new HashMap<>());
                    } else {
                        ssMBean.takeSnapshot(request.tag, new HashMap<>(), DatabaseEntities.forTakingSnapshot(request.entities));
                    }

                    return null;
                }
            });
        }
    }

    private static class TakeSnapshotOperationRequest extends OperationRequest {

        final DatabaseEntities entities;
        final String tag;

        public TakeSnapshotOperationRequest(final DatabaseEntities entities, final String tag) {
            this.entities = entities;
            this.tag = tag;
        }
    }
}
