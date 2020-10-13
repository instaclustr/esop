package com.instaclustr.esop.impl.backup.coordination;

import static com.instaclustr.esop.impl.Manifest.getLocalManifestPath;
import static com.instaclustr.esop.impl.Manifest.getManifestAsManifestEntry;
import static java.lang.String.format;

import javax.inject.Provider;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.guice.BackuperFactory;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.AbstractTracker.Session;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.KeyspaceTable;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.Snapshots;
import com.instaclustr.esop.impl.Snapshots.Snapshot;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.BackupPhaseResultGatherer;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.impl.backup.UploadTracker;
import com.instaclustr.esop.impl.backup.UploadTracker.UploadUnit;
import com.instaclustr.esop.impl.backup.coordination.ClearSnapshotOperation.ClearSnapshotOperationRequest;
import com.instaclustr.esop.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.esop.impl.interaction.CassandraTokens;
import com.instaclustr.esop.topology.CassandraClusterTopology;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationCoordinator;
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
    protected final UploadTracker uploadTracker;
    protected final Provider<CassandraVersion> cassandraVersionProvider;

    public BaseBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                          final Provider<CassandraVersion> cassandraVersionProvider,
                                          final Map<String, BackuperFactory> backuperFactoryMap,
                                          final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                          final ObjectMapper objectMapper,
                                          final UploadTracker uploadTracker) {
        this.cassandraJMXService = cassandraJMXService;
        this.backuperFactoryMap = backuperFactoryMap;
        this.bucketServiceFactoryMap = bucketServiceFactoryMap;
        this.objectMapper = objectMapper;
        this.uploadTracker = uploadTracker;
        this.cassandraVersionProvider = cassandraVersionProvider;
    }

    protected String resolveSnapshotTag(final BackupOperationRequest request, final long timestamp) {
        return format("%s-%s-%s", request.snapshotTag, request.schemaVersion, timestamp);
    }

    @Override
    public ResultGatherer<BackupOperationRequest> coordinate(final Operation<BackupOperationRequest> operation) {

        final BackupOperationRequest request = operation.request;

        logger.info(request.toString());

        final BackupPhaseResultGatherer gatherer = new BackupPhaseResultGatherer();

        Throwable cause = null;

        try {
            assert cassandraJMXService != null;
            assert backuperFactoryMap != null;
            assert bucketServiceFactoryMap != null;
            assert objectMapper != null;

            if (!request.skipBucketVerification) {
                try (final BucketService bucketService = bucketServiceFactoryMap.get(request.storageLocation.storageProvider).createBucketService(request)) {
                    bucketService.checkBucket(request.storageLocation.bucket, request.createMissingBucket);
                }
            }

            try {
                KeyspaceTable.checkEntitiesToProcess(request.cassandraDirectory.resolve("data"), request.entities);
            } catch (final Exception ex) {
                logger.error(ex.getMessage());
                return gatherer.gather(operation, ex);
            }

            final List<String> tokens = new CassandraTokens(cassandraJMXService).act();

            logger.info("Tokens {}", tokens);

            logger.info("Taking snapshot with name {}", request.snapshotTag);

            new TakeSnapshotOperation(cassandraJMXService,
                                      new TakeSnapshotOperationRequest(request.entities, request.snapshotTag),
                                      cassandraVersionProvider).run0();

            final Snapshots snapshots = Snapshots.parse(request.cassandraDirectory.resolve("data"));

            final Optional<Snapshot> snapshot = snapshots.get(request.snapshotTag);

            if (!snapshot.isPresent()) {
                throw new IllegalStateException(format("There is not any snapshot of tag %s", request.snapshotTag));
            }

            final Manifest manifest = Manifest.from(snapshot.get());

            manifest.setSchemaVersion(request.schemaVersion);
            manifest.setTokens(tokens);

            // manifest
            final Path localManifestPath = getLocalManifestPath(request.cassandraDirectory, request.snapshotTag);
            Manifest.write(manifest, localManifestPath, objectMapper);
            manifest.setManifest(getManifestAsManifestEntry(localManifestPath));

            try (final Backuper backuper = backuperFactoryMap.get(request.storageLocation.storageProvider).createBackuper(request)) {

                final List<ManifestEntry> manifestEntries = manifest.getManifestEntries();

                Session<UploadUnit> uploadSession = null;

                try {
                    uploadSession = uploadTracker.submit(backuper, operation, manifestEntries, request.snapshotTag, operation.request.concurrentConnections);

                    uploadSession.waitUntilConsideredFinished();
                    uploadTracker.cancelIfNecessary(uploadSession);
                } finally {
                    uploadTracker.removeSession(uploadSession);
                    uploadSession = null;
                }

                if (operation.request.uploadClusterTopology) {
                    final ClusterTopology topology = new CassandraClusterTopology(cassandraJMXService, operation.request.dc).act();
                    ClusterTopology.upload(backuper, topology, objectMapper, operation.request.snapshotTag);
                }
            } finally {
                manifest.cleanup();
            }
        } catch (final Exception ex) {
            logger.error("Unable to perform backup! - " + ex.getMessage(), ex);
            cause = ex;
        } finally {
            try {
                new ClearSnapshotOperation(cassandraJMXService, new ClearSnapshotOperationRequest(request.snapshotTag)).run0();
            } catch (final Exception ex) {
                logger.error(format("Unable to clear snapshot '%s' after backup!", request.snapshotTag), ex);
                if (cause == null) {
                    cause = ex;
                }
            }
        }

        return gatherer.gather(operation, cause);
    }
}
