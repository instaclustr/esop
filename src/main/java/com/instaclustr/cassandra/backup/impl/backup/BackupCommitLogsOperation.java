package com.instaclustr.cassandra.backup.impl.backup;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.guice.BackuperFactory;
import com.instaclustr.cassandra.backup.guice.BucketServiceFactory;
import com.instaclustr.cassandra.backup.impl.AbstractTracker.Session;
import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl.backup.UploadTracker.UploadUnit;
import com.instaclustr.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupCommitLogsOperation extends Operation<BackupCommitLogsOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(BackupCommitLogsOperation.class);
    private static final String CASSANDRA_COMMIT_LOGS = "commitlog";

    private final Map<String, BackuperFactory> backuperFactoryMap;
    private final Map<String, BucketServiceFactory> bucketServiceMap;
    private final UploadTracker uploadTracker;

    @AssistedInject
    public BackupCommitLogsOperation(final Map<String, BackuperFactory> backuperFactoryMap,
                                     final Map<String, BucketServiceFactory> bucketServiceMap,
                                     final UploadTracker uploadTracker,
                                     @Assisted final BackupCommitLogsOperationRequest request) {
        super(request);
        this.backuperFactoryMap = backuperFactoryMap;
        this.bucketServiceMap = bucketServiceMap;
        this.uploadTracker = uploadTracker;
    }

    @Override
    protected void run0() throws Exception {
        logger.info(request.toString());

        // generate manifest (set of object keys and source files defining the upload)
        final Collection<ManifestEntry> manifestEntries = new LinkedList<>(); // linked list to maintain order
        final Pattern pattern = Pattern.compile("CommitLog-\\d+-\\d+\\.log");
        final DirectoryStream.Filter<Path> filter = entry -> Files.isRegularFile(entry) && pattern.matcher(entry.getFileName().toString()).matches();

        final Path commitLogArchiveDirectory = resolveCommitLogsPath(request);
        final Path backupCommitLogRootKey = Paths.get(CASSANDRA_COMMIT_LOGS);

        try (final DirectoryStream<Path> commitLogs = Files.newDirectoryStream(commitLogArchiveDirectory, filter);
            final Backuper backuper = backuperFactoryMap.get(request.storageLocation.storageProvider).createCommitLogBackuper(request);
            final BucketService bucketService = bucketServiceMap.get(request.storageLocation.storageProvider).createBucketService(request)) {

            if (request.createMissingBucket) {
                bucketService.createIfMissing(request.storageLocation.bucket);
            }

            for (final Path commitLog : commitLogs) {
                // Append file modified date so we have some idea of the time range this commitlog covers

                // millisecond precision, on *nix, it trims milliseconds and returns "000" instead
                // when using File.lastModified
                long commitLogLastModified = Files.getLastModifiedTime(commitLog.toFile().toPath()).toMillis();

                final Path bucketKey = backupCommitLogRootKey.resolve(commitLog.getFileName().toString() + "." + commitLogLastModified);
                manifestEntries.add(new ManifestEntry(bucketKey, commitLog, ManifestEntry.Type.FILE));
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

    private Path resolveCommitLogsPath(final BackupCommitLogsOperationRequest request) {
        if (request.commitLogArchiveOverride != null && request.commitLogArchiveOverride.toFile().exists()) {
            return request.commitLogArchiveOverride;
        } else {
            return request.cassandraDirectory.resolve(CASSANDRA_COMMIT_LOGS);
        }
    }
}
