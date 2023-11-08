package com.instaclustr.esop.impl.backup.coordination;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.guice.BackuperFactory;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.AbstractTracker.Session;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.CassandraData;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.Snapshots;
import com.instaclustr.esop.impl.Snapshots.Snapshot;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.impl.backup.BaseBackupOperationRequest;
import com.instaclustr.esop.impl.backup.UploadTracker;
import com.instaclustr.esop.impl.backup.UploadTracker.UploadUnit;
import com.instaclustr.esop.impl.backup.coordination.ClearSnapshotOperation.ClearSnapshotOperationRequest;
import com.instaclustr.esop.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.esop.impl.interaction.CassandraTokens;
import com.instaclustr.esop.topology.CassandraClusterTopology;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.esop.topology.CassandraSimpleTopology;
import com.instaclustr.esop.topology.CassandraSimpleTopology.CassandraSimpleTopologyResult;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.Error;
import com.instaclustr.operations.OperationCoordinator;
import jmx.org.apache.cassandra.service.CassandraJMXService;

import static com.instaclustr.esop.impl.Manifest.getLocalManifestPath;
import static com.instaclustr.esop.impl.Manifest.getManifestAsManifestEntry;
import static java.lang.String.format;

public class BaseBackupOperationCoordinator extends OperationCoordinator<BackupOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(BaseBackupOperationCoordinator.class);

    protected final CassandraJMXService cassandraJMXService;
    protected final Map<String, BackuperFactory> backuperFactoryMap;
    protected final Map<String, BucketServiceFactory> bucketServiceFactoryMap;
    protected final ObjectMapper objectMapper;
    protected final UploadTracker uploadTracker;
    protected final Provider<CassandraVersion> cassandraVersionProvider;
    protected final HashSpec hashSpec;

    public BaseBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                          final Provider<CassandraVersion> cassandraVersionProvider,
                                          final Map<String, BackuperFactory> backuperFactoryMap,
                                          final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                          final ObjectMapper objectMapper,
                                          final UploadTracker uploadTracker,
                                          final HashSpec hashSpec) {
        this.cassandraJMXService = cassandraJMXService;
        this.backuperFactoryMap = backuperFactoryMap;
        this.bucketServiceFactoryMap = bucketServiceFactoryMap;
        this.objectMapper = objectMapper;
        this.uploadTracker = uploadTracker;
        this.cassandraVersionProvider = cassandraVersionProvider;
        this.hashSpec = hashSpec;
    }

    protected String resolveSnapshotTag(final BackupOperationRequest request, final long timestamp) {
        return format("%s-%s-%s", request.snapshotTag, request.schemaVersion, timestamp);
    }

    @Override
    public void coordinate(final Operation<BackupOperationRequest> operation) {

        final BackupOperationRequest request = operation.request;

        logger.info(request.toString());

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

            final CassandraData cassandraData = CassandraData.parse(request.dataDirs.get(0));
            cassandraData.setDatabaseEntitiesFromRequest(request.entities);

            if (request.storageLocation.incompleteNodeLocation()) {
                CassandraSimpleTopologyResult result = new CassandraSimpleTopology(cassandraJMXService).act();
                request.storageLocation = StorageLocation.update(request.storageLocation,
                                                                 result.getClusterName(),
                                                                 result.getDc(),
                                                                 result.getHostId());

                logger.info("Storage location was updated to " + request.storageLocation.rawLocation);
            }

            final List<String> tokens = new CassandraTokens(cassandraJMXService).act();

            logger.info("Tokens {}", tokens);

            if (!Snapshots.snapshotContainsTimestamp(operation.request.snapshotTag)) {
                if (operation.request.schemaVersion == null) {
                    operation.request.schemaVersion = new CassandraSchemaVersion(cassandraJMXService).act();
                }
                operation.request.snapshotTag = resolveSnapshotTag(operation.request, System.currentTimeMillis());
            }

            logger.info("Taking snapshot with name {}", request.snapshotTag);

            new TakeSnapshotOperation(cassandraJMXService,
                                      new TakeSnapshotOperationRequest(request.entities, request.snapshotTag),
                                      cassandraVersionProvider).run0();

            Snapshots.hashSpec = hashSpec;
            final Snapshots snapshots = Snapshots.parse(request.dataDirs, request.snapshotTag);
            final Optional<Snapshot> snapshot = snapshots.get(request.snapshotTag);

            if (!snapshot.isPresent()) {
                throw new IllegalStateException(format("There is not any snapshot of tag %s", request.snapshotTag));
            }

            final Manifest manifest = Manifest.from(snapshot.get());

            manifest.setSchemaVersion(request.schemaVersion);
            manifest.setTokens(tokens);

            // manifest
            final Path localManifestPath = getLocalManifestPath(request.snapshotTag);
            manifest.setManifest(getManifestAsManifestEntry(localManifestPath, request));

            try (final Backuper backuper = backuperFactoryMap.get(request.storageLocation.storageProvider).createBackuper(request)) {

                performUpload(manifest.getManifestEntries(false), backuper, operation, request);

                // upload manifest as the last, possibly with updated file sizes as they were encrypted
                backuper.uploadText(objectMapper.writeValueAsString(manifest),
                                    backuper.objectKeyToNodeAwareRemoteReference(manifest.getManifest().objectKey));

                if (operation.request.uploadClusterTopology) {
                    // here we will upload all topology because we do not know what restore might look like (what dc a restorer will restore against if any)
                    final ClusterTopology topology = new CassandraClusterTopology(cassandraJMXService, null).act();
                    ClusterTopology.upload(backuper, topology, objectMapper, operation.request.snapshotTag);
                }
            }
        } catch (final Exception ex) {
            operation.addError(Error.from(ex));
        } finally {
            final ClearSnapshotOperation cso = new ClearSnapshotOperation(cassandraJMXService, new ClearSnapshotOperationRequest(request.snapshotTag));
            try {
                cso.run0();
            } catch (final Exception ex) {
                operation.addErrors(cso.errors);
            }
        }
    }

    private void performUpload(List<ManifestEntry> manifestEntries,
                               Backuper backuper,
                               Operation<? extends BaseBackupOperationRequest> operation,
                               BackupOperationRequest request) throws Exception {
        Session<UploadUnit> uploadSession = null;

        try {
            uploadSession = uploadTracker.submit(backuper,
                                                 operation,
                                                 manifestEntries,
                                                 request.snapshotTag,
                                                 operation.request.concurrentConnections);

            uploadSession.waitUntilConsideredFinished();
            uploadTracker.cancelIfNecessary(uploadSession);

            final List<UploadUnit> failedUnits = uploadSession.getFailedUnits();

            if (!failedUnits.isEmpty()) {
                final String message = failedUnits.stream().map(unit -> unit.getManifestEntry().objectKey.toString()).collect(Collectors.joining(","));
                logger.error(message);
                throw new IOException(format("Unable to upload some files successfully: %s", message));
            }
        } finally {
            uploadTracker.removeSession(uploadSession);
        }
    }
}
