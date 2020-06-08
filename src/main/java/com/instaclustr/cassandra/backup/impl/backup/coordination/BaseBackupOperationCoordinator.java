package com.instaclustr.cassandra.backup.impl.backup.coordination;

import static com.instaclustr.cassandra.backup.impl.Manifest.getLocalManifestPath;
import static com.instaclustr.cassandra.backup.impl.Manifest.getManifestAsManifestEntry;
import static java.lang.String.format;

import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.cassandra.backup.guice.BackuperFactory;
import com.instaclustr.cassandra.backup.guice.BucketServiceFactory;
import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.Manifest;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl.Snapshots;
import com.instaclustr.cassandra.backup.impl.Snapshots.Snapshot;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupPhaseResultGatherer;
import com.instaclustr.cassandra.backup.impl.backup.Backuper;
import com.instaclustr.cassandra.backup.impl.backup.coordination.ClearSnapshotOperation.ClearSnapshotOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraTokens;
import com.instaclustr.io.GlobalLock;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.OperationProgressTracker;
import com.instaclustr.operations.ResultGatherer;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseBackupOperationCoordinator extends OperationCoordinator<BackupOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(BaseBackupOperationCoordinator.class);

    protected final CassandraJMXService cassandraJMXService;
    protected final Map<String, BackuperFactory> backuperFactoryMap;
    protected final Map<String, BucketServiceFactory> bucketServiceFactoryMap;
    protected final ObjectMapper objectMapper;

    public BaseBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                          final Map<String, BackuperFactory> backuperFactoryMap,
                                          final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                          final ObjectMapper objectMapper) {
        this.cassandraJMXService = cassandraJMXService;
        this.backuperFactoryMap = backuperFactoryMap;
        this.bucketServiceFactoryMap = bucketServiceFactoryMap;
        this.objectMapper = objectMapper;
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
            assert objectMapper != null;

            fileLock = new GlobalLock(request.lockFile).waitForLock();

            if (!request.keepExistingSnapshot) {
                new ClearSnapshotOperation(cassandraJMXService, new ClearSnapshotOperationRequest(request.snapshotTag)).run0();
            }

            new TakeSnapshotOperation(cassandraJMXService, new TakeSnapshotOperationRequest(request.entities, request.snapshotTag)).run0();

            final List<String> tokens = new CassandraTokens(cassandraJMXService).act();

            logger.info("Tokens " + tokens);

            final String schemaVersion = new CassandraSchemaVersion(cassandraJMXService).act();

            logger.info("Schema version " + schemaVersion);

            final Snapshots snapshots = Snapshots.parse(request.cassandraDirectory.resolve("data"));

            final Optional<Snapshot> snapshot = snapshots.get(request.snapshotTag);

            if (!snapshot.isPresent()) {
                throw new IllegalStateException(format("There is not any snapshot of tag %s", request.snapshotTag));
            }

            final Manifest manifest = Manifest.from(snapshot.get());

            manifest.setSchemaVersion(schemaVersion);
            manifest.setTokens(tokens);

            // manifest
            final Path localManifestPath = getLocalManifestPath(request.cassandraDirectory, request.snapshotTag, schemaVersion);
            Manifest.write(manifest, localManifestPath, objectMapper);
            manifest.setManifest(getManifestAsManifestEntry(localManifestPath));

            BucketService bucketService = null;
            Backuper backuper = null;

            try {
                bucketService = bucketServiceFactoryMap.get(request.storageLocation.storageProvider).createBucketService(request);

                if (request.createMissingBucket) {
                    bucketService.createIfMissing(request.storageLocation.bucket);
                }

                backuper = backuperFactoryMap.get(request.storageLocation.storageProvider).createBackuper(request);

                final List<ManifestEntry> manifestEntries = manifest.getManifestEntries();

                backuper.uploadOrFreshenFiles(operation, manifestEntries, new OperationProgressTracker(operation, manifestEntries.size()));
            } finally {
                if (bucketService != null) {
                    bucketService.close();
                }

                if (backuper != null) {
                    backuper.close();
                }

                manifest.cleanup();
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

}
