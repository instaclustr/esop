package com.instaclustr.cassandra.backup.impl.restore;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.util.concurrent.Futures;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.operations.OperationProgressTracker;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.StorageInteractor;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Restorer extends StorageInteractor {

    private static final Logger logger = LoggerFactory.getLogger(Restorer.class);

    protected final BaseRestoreOperationRequest request;
    private final ExecutorServiceSupplier executorServiceSupplier;

    public Restorer(final BaseRestoreOperationRequest request,
                    final ExecutorServiceSupplier executorServiceSupplier) {
        super(request.storageLocation);
        this.request = request;
        this.executorServiceSupplier = executorServiceSupplier;

    }

    public void downloadManifestEntry(final ManifestEntry manifestEntry) throws Exception {
        this.downloadFile(manifestEntry.localFile, objectKeyToRemoteReference(manifestEntry.objectKey));
    }

    public abstract String downloadFileToString(final Path localPath, final RemoteObjectReference objectReference) throws Exception;

    public abstract void downloadFile(final Path localPath, final RemoteObjectReference objectReference) throws Exception;

    public abstract Path downloadFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception;

    public void downloadFiles(final Collection<ManifestEntry> manifest,
                              final OperationProgressTracker operationProgressTracker) throws Exception {

        if (manifest.isEmpty()) {
            operationProgressTracker.complete();
            logger.info("0 files to download.");
            return;
        }

        logger.info("{} files to download.", manifest.size());

        final CountDownLatch completionLatch = new CountDownLatch(manifest.size());

        final ExecutorService executorService = executorServiceSupplier.get(request.concurrentConnections);

        final Iterable<Future<?>> downloadResults = manifest.stream().map((entry) -> {
            try {
                return executorService.submit(() -> {
                    RemoteObjectReference remoteObjectReference = objectKeyToRemoteReference(entry.objectKey);
                    try {
                        logger.info(String.format("Downloading file %s to %s. %s files to go.", remoteObjectReference.getObjectKey(), entry.localFile, completionLatch.getCount()));

                        Path localPath = entry.localFile;

                        if (remoteObjectReference.canonicalPath.endsWith("-schema.cql")) {
                            localPath = entry.localFile.getParent().resolve("schema.cql");
                        }

                        this.downloadFile(localPath, remoteObjectReference);

                        logger.info(String.format("Successfully downloaded file %s to %s.", remoteObjectReference.getObjectKey(), localPath));

                        return null;
                    } catch (final Throwable t) {
                        logger.error(String.format("Failed to download file %s.", remoteObjectReference.getObjectKey()), t);

                        executorService.shutdownNow(); // prevent new tasks or other tasks from running

                        throw t;
                    } finally {
                        operationProgressTracker.update();
                        completionLatch.countDown();
                    }
                });
            } catch (final RejectedExecutionException e) {
                return Futures.immediateFailedFuture(e);
            }
        }).collect(toList());

        // wait for uploads to finish
        executorService.shutdown();

        while (true) {
            if (executorService.awaitTermination(1, MINUTES)) {
                break;
            }
        }

        // rethrow any exception caused by a download task so we exit with failure
        for (final Future<?> result : downloadResults) {
            result.get();
        }
    }
}
