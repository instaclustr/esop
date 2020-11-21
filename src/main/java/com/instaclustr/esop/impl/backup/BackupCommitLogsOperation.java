package com.instaclustr.esop.impl.backup;

import static com.instaclustr.esop.impl.ManifestEntry.Type.FILE;

import java.net.InetAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.guice.BackuperFactory;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.AbstractTracker.Session;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.UploadTracker.UploadUnit;
import com.instaclustr.esop.impl.interaction.CassandraMyEndpoint;
import com.instaclustr.esop.topology.CassandraClusterName;
import com.instaclustr.esop.topology.CassandraEndpointDC;
import com.instaclustr.esop.topology.CassandraEndpoints;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupCommitLogsOperation extends Operation<BackupCommitLogsOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(BackupCommitLogsOperation.class);
    private static final Path CASSANDRA_COMMITLOG = Paths.get("commitlog");

    private final Map<String, BackuperFactory> backuperFactoryMap;
    private final Map<String, BucketServiceFactory> bucketServiceMap;
    private final UploadTracker uploadTracker;
    private final CassandraJMXService cassandraJMXService;

    @AssistedInject
    public BackupCommitLogsOperation(final Map<String, BackuperFactory> backuperFactoryMap,
                                     final Map<String, BucketServiceFactory> bucketServiceMap,
                                     final UploadTracker uploadTracker,
                                     final CassandraJMXService cassandraJMXService,
                                     @Assisted final BackupCommitLogsOperationRequest request) {
        super(request);
        this.backuperFactoryMap = backuperFactoryMap;
        this.bucketServiceMap = bucketServiceMap;
        this.uploadTracker = uploadTracker;
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    protected void run0() throws Exception {

        updateStorageLocationIfNecessary();

        logger.info(request.toString());

        // generate manifest (set of object keys and source files defining the upload)
        final Collection<ManifestEntry> manifestEntries = new LinkedList<>(); // linked list to maintain order

        try (final DirectoryStream<Path> commitLogs = getCommitLogs(request);
            final Backuper backuper = backuperFactoryMap.get(request.storageLocation.storageProvider).createCommitLogBackuper(request);
            final BucketService bucketService = bucketServiceMap.get(request.storageLocation.storageProvider).createBucketService(request)) {

            if (!request.skipBucketVerification) {
                bucketService.checkBucket(request.storageLocation.bucket, request.createMissingBucket);
            }

            for (final Path commitLog : commitLogs) {
                // Append file modified date so we have some idea of the time range this commitlog covers

                // millisecond precision, on *nix, it trims milliseconds and returns "000" instead
                // when using File.lastModified
                long commitLogLastModified = Files.getLastModifiedTime(commitLog.toFile().toPath()).toMillis();

                final Path bucketKey = CASSANDRA_COMMITLOG.resolve(commitLog.getFileName().toString() + "." + commitLogLastModified);

                manifestEntries.add(new ManifestEntry(bucketKey, commitLog, FILE));
            }

            logger.debug("{} files in manifest for commitlog backup.", manifestEntries.size());

            Session<UploadUnit> uploadSession = null;

            try {
                uploadSession = uploadTracker.submit(backuper, this, manifestEntries, null, this.request.concurrentConnections);
                uploadSession.waitUntilConsideredFinished();
                uploadTracker.cancelIfNecessary(uploadSession);
            } finally {
                uploadTracker.removeSession(uploadSession);
            }
        }
    }

    private void updateStorageLocationIfNecessary() throws Exception {
        if (!request.online) {
            return;
        }

        final String clusterName = new CassandraClusterName(cassandraJMXService).act();

        final String myHostId = new CassandraMyEndpoint(cassandraJMXService).act();

        Optional<InetAddress> address = new CassandraEndpoints(cassandraJMXService).act()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().equals(UUID.fromString(myHostId)))
            .map(Entry::getKey)
            .findFirst();

        if (!address.isPresent()) {
            throw new IllegalStateException("Unable to retrieve inet address of Cassandra host!");
        }

        final String datacenter = new CassandraEndpointDC(cassandraJMXService, address.get()).act().get(address.get());

        logger.info("Resolved cluster name: {} ", clusterName);
        logger.info("Resolved datacenter: {} ", datacenter);
        logger.info("Resolved host id: {} ", myHostId);

        request.storageLocation = StorageLocation.update(request.storageLocation, clusterName, datacenter, myHostId);
    }

    private DirectoryStream<Path> getCommitLogs(final BackupCommitLogsOperationRequest request) throws Exception {
        if (request.commitLog != null && !request.commitLog.toFile().getAbsolutePath().equals("/")) {
            return Files.newDirectoryStream(request.commitLog.getParent(),
                                            entry -> entry.getFileName().toString().equals(request.commitLogArchiveOverride.toFile().getName()));
        } else {
            final Pattern pattern = Pattern.compile("CommitLog-\\d+-\\d+\\.log");
            return Files.newDirectoryStream(resolveCommitLogsPath(request),
                                            entry -> Files.isRegularFile(entry) && pattern.matcher(entry.getFileName().toString()).matches());
        }
    }

    private Path resolveCommitLogsPath(final BackupCommitLogsOperationRequest request) {
        if (request.commitLogArchiveOverride != null && request.commitLogArchiveOverride.toFile().exists()) {
            return request.commitLogArchiveOverride;
        } else {
            return request.cassandraDirectory.resolve(CASSANDRA_COMMITLOG);
        }
    }
}
