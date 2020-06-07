package com.instaclustr.cassandra.backup.impl.backup;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.RateLimiter;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl.ManifestEntry.Type;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.StorageInteractor;
import com.instaclustr.io.RateLimitedInputStream;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.DataSize;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationProgressTracker;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Backuper extends StorageInteractor {

    private static final Logger logger = LoggerFactory.getLogger(Backuper.class);

    protected final BaseBackupOperationRequest request;
    private final ExecutorServiceSupplier executorServiceSupplier;

    protected Backuper(final BaseBackupOperationRequest request,
                       final ExecutorServiceSupplier executorServiceSupplier) {
        super(request.storageLocation);
        this.request = request;
        this.executorServiceSupplier = executorServiceSupplier;
    }

    public enum FreshenResult {
        FRESHENED,
        UPLOAD_REQUIRED
    }

    public abstract FreshenResult freshenRemoteObject(final RemoteObjectReference object) throws Exception;

    public abstract void uploadFile(final long size,
                                    final InputStream localFileStream,
                                    final RemoteObjectReference object) throws Exception;

    private static final class FileUploadException extends RuntimeException {

        public FileUploadException(final Throwable t) {
            super(t);
        }
    }

    private static final class ManifestEntryDownload implements Supplier<Void> {

        private final ManifestEntry manifestEntry;
        private final Backuper backuper;
        private final OperationProgressTracker operationProgressTracker;
        private final AtomicBoolean shouldCancel;

        public ManifestEntryDownload(final Backuper backuper,
                                     final ManifestEntry manifestEntry,
                                     final OperationProgressTracker operationProgressTracker,
                                     final AtomicBoolean shouldCancel) {
            this.manifestEntry = manifestEntry;
            this.backuper = backuper;
            this.operationProgressTracker = operationProgressTracker;
            this.shouldCancel = shouldCancel;
        }

        @Override
        public Void get() {
            try (final InputStream fileStream = new BufferedInputStream(new FileInputStream(manifestEntry.localFile.toFile()))) {

                final RemoteObjectReference remoteObjectReference = backuper.objectKeyToRemoteReference(manifestEntry.objectKey);

                try {
                    if (manifestEntry.type != Type.MANIFEST_FILE && backuper.freshenRemoteObject(remoteObjectReference) == Backuper.FreshenResult.FRESHENED) {
                        logger.debug("Skipping the upload of already uploaded file {}", remoteObjectReference.canonicalPath);
                        return null;
                    }

                } catch (final Exception ex) {
                    logger.warn("Failed to freshen file '{}'.", manifestEntry.objectKey, ex);
                    throw ex;
                }

                final InputStream rateLimitedStream = backuper.getUploadingInputStreamFunction(shouldCancel).apply(fileStream);

                logger.info("Uploading file '{}' ({}).", manifestEntry.objectKey, DataSize.bytesToHumanReadable(manifestEntry.size));
                backuper.uploadFile(manifestEntry.size, rateLimitedStream, remoteObjectReference);
            } catch (final Throwable t) {
                logger.error(format("Failed to upload file '%s", manifestEntry.objectKey), t);
                shouldCancel.set(true);
                throw new FileUploadException(t);
            } finally {
                operationProgressTracker.update();
            }

            return null;
        }
    }

    public void uploadOrFreshenFiles(final Operation<?> operation,
                                     final Collection<ManifestEntry> manifest,
                                     final OperationProgressTracker operationProgressTracker) throws Exception {
        if (manifest.isEmpty()) {
            operationProgressTracker.complete();
            logger.info("0 files to upload.");
            return;
        }

        final long filesSizeSum = getFilesSizeSum(manifest);

        computeBPS(request, filesSizeSum);

        logger.info("{} files to upload. Total size {}.", manifest.size(), DataSize.bytesToHumanReadable(filesSizeSum));

        final ExecutorService executorService = executorServiceSupplier.get(request.concurrentConnections);

        final Collection<ManifestEntry> noManifestEntries = manifest.stream().filter(entry -> entry.type != Type.MANIFEST_FILE).collect(Collectors.toSet());
        final Optional<ManifestEntry> manifestEntry = manifest.stream().filter(entry -> entry.type == Type.MANIFEST_FILE).findFirst();

        final Collection<ManifestEntryDownload> entriesToDownload = new ArrayList<>();

        // this might throw
        for (final ManifestEntry entry : noManifestEntries) {
            entriesToDownload.add(new ManifestEntryDownload(this, entry, operationProgressTracker, operation.getShouldCancel()));
        }

        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());

        allOf(entriesToDownload.stream().map(etd -> supplyAsync(etd, executorService).whenComplete((r, t) -> {
            if (t != null) {
                exceptions.add(t);
            }
        })).toArray(CompletableFuture<?>[]::new)).get();

        if (exceptions.isEmpty() && !operation.getShouldCancel().get()) {
            manifestEntry.ifPresent(entry -> new ManifestEntryDownload(this, entry, operationProgressTracker, operation.getShouldCancel()).get());
        }

        executorService.shutdown();

        while (true) {
            if (executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                break;
            }
        }

        if (!exceptions.isEmpty()) {
            throw new FileUploadException(exceptions.get(0));
        }
    }

    private long getFilesSizeSum(final Collection<ManifestEntry> manifestEntries) {
        return manifestEntries.stream().map(e -> e.size).reduce(0L, Long::sum);
    }

    private void computeBPS(final BaseBackupOperationRequest request, final long filesSizeSum) {
        if (request.duration != null) {
            long bps = filesSizeSum / request.duration.asSeconds().value;
            if (request.bandwidth != null) {
                bps = Math.min(request.bandwidth.asBytesPerSecond().value, bps);
            }

            bps = Math.max(new DataRate(500L, DataRate.DataRateUnit.KBPS).asBytesPerSecond().value, bps);

            request.bandwidth = new DataRate(bps, DataRate.DataRateUnit.BPS);
        }
    }

    private Function<InputStream, InputStream> getUploadingInputStreamFunction(final AtomicBoolean shouldCancel) {
        return request.bandwidth == null ? identity() : inputStream -> {
            final RateLimiter rateLimiter = RateLimiter.create(request.bandwidth.asBytesPerSecond().value);
            logger.info("Upload bandwidth capped at {}.", request.bandwidth);
            return new RateLimitedInputStream(inputStream, rateLimiter, shouldCancel);
        };
    }
}
