package com.instaclustr.esop.impl.backup;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.instaclustr.esop.impl.AbstractTracker;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupModules.UploadingFinisher;
import com.instaclustr.esop.impl.backup.UploadTracker.UploadSession;
import com.instaclustr.esop.impl.backup.UploadTracker.UploadUnit;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.retry.Retrier.RetriableException;
import com.instaclustr.io.RateLimitedInputStream;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.DataRate.DataRateUnit;
import com.instaclustr.measure.DataSize;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.instaclustr.esop.impl.ManifestEntry.Type.MANIFEST_FILE;
import static com.instaclustr.esop.impl.backup.Backuper.FreshenResult.FRESHENED;
import static com.instaclustr.esop.impl.retry.RetrierFactory.getRetrier;
import static java.lang.String.format;
import static java.util.function.Function.identity;

public class UploadTracker extends AbstractTracker<UploadUnit, UploadSession, Backuper, BaseBackupOperationRequest> {

    @Inject
    public UploadTracker(final @UploadingFinisher ListeningExecutorService finisherExecutorService,
                         final OperationsService operationsService,
                         final HashSpec hashSpec) {
        super(finisherExecutorService, operationsService, hashSpec);
    }

    @Override
    public UploadUnit constructUnitToSubmit(final Backuper backuper,
                                            final ManifestEntry manifestEntry,
                                            final AtomicBoolean shouldCancel,
                                            final String snapshotTag,
                                            final HashSpec hashSpec) {
        return new UploadUnit(backuper, manifestEntry, shouldCancel, snapshotTag, hashSpec);
    }

    @Override
    public Session<UploadUnit> constructSession() {
        return new UploadSession();
    }

    @Override
    public Session<UploadUnit> submit(final Backuper backuper,
                                      final Operation<? extends BaseBackupOperationRequest> operation,
                                      final Collection<ManifestEntry> entries,
                                      final String snapshotTag,
                                      final int concurrentConnections) {
        final long filesSizeSum = getFilesSizeSum(entries);
        computeBPS(backuper.request, filesSizeSum, concurrentConnections);
        return super.submit(backuper,
                            operation,
                            entries,
                            snapshotTag,
                            concurrentConnections);
    }

    public static class UploadSession extends AbstractTracker.Session<UploadUnit> {

    }

    public static class UploadUnit extends AbstractTracker.Unit {

        private static final Logger logger = LoggerFactory.getLogger(UploadUnit.class);

        @JsonIgnore
        private final Backuper backuper;

        @JsonIgnore
        private String snapshotTag;

        public UploadUnit(final Backuper backuper,
                          final ManifestEntry manifestEntry,
                          final AtomicBoolean shouldCancel,
                          final String snapshotTag,
                          final HashSpec hashSpec) {
            super(manifestEntry, shouldCancel, hashSpec);
            this.backuper = backuper;
            this.snapshotTag = snapshotTag;
        }


        @Override
        public Void call() {
            try {
                state = State.RUNNING;

                final RemoteObjectReference ref = backuper.objectKeyToNodeAwareRemoteReference(manifestEntry.objectKey);

                // try to refresh object / decide if it is required to upload it
                Callable<Boolean> condition = () -> {
                    try {
                        return backuper.freshenRemoteObject(manifestEntry, ref) == FRESHENED;
                    } catch (final Exception ex) {
                        throw new RetriableException("Failed to refresh remote object" + ref.objectKey, ex);
                    }
                };

                if (manifestEntry.type != MANIFEST_FILE && getRetrier(backuper.request.retry).submit(condition)) {
                    logger.info("{}skipping the upload of already uploaded file {}",
                                snapshotTag != null ? "Snapshot " + snapshotTag + " - " : "",
                                ref.canonicalPath);

                    state = State.FINISHED;
                    return null;
                }

                // do the upload
                getRetrier(backuper.request.retry).submit(() -> {
                    try (final InputStream fileStream = new BufferedInputStream(Files.newInputStream(manifestEntry.localFile.toFile().toPath()))) {
                        final InputStream rateLimitedStream = getUploadingInputStreamFunction(backuper.request).apply(fileStream);

                        logger.debug(format("%suploading file '%s' (%s).",
                                            snapshotTag != null ? "Snapshot " + snapshotTag + " - " : "",
                                            manifestEntry.objectKey,
                                            DataSize.bytesToHumanReadable(manifestEntry.size)));
                        // never encrypt manifest
                        if (manifestEntry.type == MANIFEST_FILE) {
                            backuper.uploadFile(manifestEntry, rateLimitedStream, ref);
                        } else {
                            backuper.uploadEncryptedFile(manifestEntry, rateLimitedStream, ref);
                        }
                    } catch (final Exception ex) {
                        throw new RetriableException(String.format("Retrying upload of %s", manifestEntry.objectKey), ex);
                    }
                });

                state = State.FINISHED;

            } catch (final Throwable t) {
                state = State.FAILED;
                t.printStackTrace();

                Throwable cause = t;
                if (t.getCause() != null)
                    cause = t.getCause();

                logger.error(format("Failed to upload file '%s'", manifestEntry.objectKey), cause.getMessage());
                shouldCancel.set(true);
                throwable = t;
            }

            return null;
        }

        private Function<InputStream, InputStream> getUploadingInputStreamFunction(final BaseBackupOperationRequest request) {
            return request.bandwidth == null ? identity() : inputStream -> {
                final RateLimiter rateLimiter = RateLimiter.create(request.bandwidth.asBytesPerSecond().value);
                return new RateLimitedInputStream(inputStream, rateLimiter, shouldCancel);
            };
        }
    }

    private long getFilesSizeSum(final Collection<ManifestEntry> manifestEntries) {
        return manifestEntries.stream().map(e -> e.size).reduce(0L, Long::sum);
    }

    private void computeBPS(final BaseBackupOperationRequest request, final long filesSizeSum, final int concurrentConnections) {

        long bpsFromBandwidth = 0;
        long bpsFromDuration = 0;

        if (request.bandwidth != null) {
            bpsFromBandwidth = request.bandwidth.asBytesPerSecond().value;
        }

        if (request.duration != null) {
            bpsFromDuration = filesSizeSum / request.duration.asSeconds().value;
        }

        if (bpsFromBandwidth != 0 || bpsFromDuration != 0) {
            long bps = Math.max(bpsFromBandwidth, bpsFromDuration) / concurrentConnections;
            logger.info("BPS computed to be {}", bps);
            request.bandwidth = new DataRate(bps, DataRateUnit.BPS);
        }
    }
}
