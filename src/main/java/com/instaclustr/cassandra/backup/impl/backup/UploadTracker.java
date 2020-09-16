package com.instaclustr.cassandra.backup.impl.backup;

import static com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit.State.FAILED;
import static com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit.State.FINISHED;
import static com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit.State.RUNNING;
import static com.instaclustr.cassandra.backup.impl.ManifestEntry.Type.MANIFEST_FILE;
import static java.lang.String.format;
import static java.util.function.Function.identity;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.instaclustr.cassandra.backup.impl.AbstractTracker;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.backup.BackupModules.UploadingFinisher;
import com.instaclustr.cassandra.backup.impl.backup.UploadTracker.UploadSession;
import com.instaclustr.cassandra.backup.impl.backup.UploadTracker.UploadUnit;
import com.instaclustr.io.RateLimitedInputStream;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.DataRate.DataRateUnit;
import com.instaclustr.measure.DataSize;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadTracker extends AbstractTracker<UploadUnit, UploadSession, Backuper, BaseBackupOperationRequest> {

    @Inject
    public UploadTracker(final @UploadingFinisher ListeningExecutorService finisherExecutorService,
                         final OperationsService operationsService) {
        super(finisherExecutorService, operationsService);
    }

    @Override
    public UploadUnit constructUnitToSubmit(final Backuper backuper,
                                            final ManifestEntry manifestEntry,
                                            final AtomicBoolean shouldCancel,
                                            final String snapshotTag) {
        return new UploadUnit(backuper, manifestEntry, shouldCancel, snapshotTag);
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
        computeBPS(backuper.request, filesSizeSum);
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
                          final String snapshotTag) {
            super(manifestEntry, shouldCancel);
            this.backuper = backuper;
            this.snapshotTag = snapshotTag;
        }

        @Override
        public Void call() {

            state = RUNNING;

            try (final InputStream fileStream = new BufferedInputStream(new FileInputStream(manifestEntry.localFile.toFile()))) {

                final RemoteObjectReference remoteObjectReference = backuper.objectKeyToNodeAwareRemoteReference(manifestEntry.objectKey);

                try {
                    if (manifestEntry.type != MANIFEST_FILE && backuper.freshenRemoteObject(remoteObjectReference) == Backuper.FreshenResult.FRESHENED) {
                        logger.info(format("%sskipping the upload of already uploaded file %s",
                                           snapshotTag != null ? "Snapshot " + snapshotTag + " - " : "",
                                           remoteObjectReference.canonicalPath));

                        state = FINISHED;
                        return null;
                    }

                } catch (final Exception ex) {
                    logger.warn("Failed to freshen file '{}'.", manifestEntry.objectKey, ex);
                    throw ex;
                }

                final InputStream rateLimitedStream = getUploadingInputStreamFunction(backuper.request).apply(fileStream);

                logger.info(String.format("%suploading file '%s' (%s).",
                                          snapshotTag != null ? "Snapshot " + snapshotTag + " - " : "",
                                          manifestEntry.objectKey,
                                          DataSize.bytesToHumanReadable(manifestEntry.size)));
                backuper.uploadFile(manifestEntry.size, rateLimitedStream, remoteObjectReference);

                state = FINISHED;
            } catch (final Throwable t) {
                state = FAILED;
                logger.error(format("Failed to upload file '%s", manifestEntry.objectKey), t);
                shouldCancel.set(true);
                this.throwable = t;
            }

            return null;
        }

        private Function<InputStream, InputStream> getUploadingInputStreamFunction(final BaseBackupOperationRequest request) {
            return request.bandwidth == null ? identity() : inputStream -> {
                final RateLimiter rateLimiter = RateLimiter.create(request.bandwidth.asBytesPerSecond().value);
                logger.debug("Upload bandwidth capped at {}.", request.bandwidth);
                return new RateLimitedInputStream(inputStream, rateLimiter, shouldCancel);
            };
        }
    }

    private long getFilesSizeSum(final Collection<ManifestEntry> manifestEntries) {
        return manifestEntries.stream().map(e -> e.size).reduce(0L, Long::sum);
    }

    private void computeBPS(final BaseBackupOperationRequest request, final long filesSizeSum) {

        long bpsFromBandwidth = 0;
        long bpsFromDuration = 0;

        if (request.bandwidth != null) {
            bpsFromBandwidth = request.bandwidth.asBytesPerSecond().value;
        }

        if (request.duration != null) {
            bpsFromDuration = filesSizeSum / request.duration.asSeconds().value;
        }

        if (bpsFromBandwidth != 0 || bpsFromDuration != 0) {
            long bps = Math.max(bpsFromBandwidth, bpsFromDuration);
            request.bandwidth = new DataRate(bps, DataRateUnit.BPS);
        }
    }
}
