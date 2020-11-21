package com.instaclustr.esop.impl.restore;

import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.FAILED;
import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.FINISHED;
import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.RUNNING;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import com.instaclustr.esop.impl.AbstractTracker;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.restore.DownloadTracker.DownloadSession;
import com.instaclustr.esop.impl.restore.DownloadTracker.DownloadUnit;
import com.instaclustr.esop.impl.restore.RestoreModules.DownloadingFinisher;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadTracker extends AbstractTracker<DownloadUnit, DownloadSession, Restorer, BaseRestoreOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(DownloadTracker.class);

    @Inject
    public DownloadTracker(final @DownloadingFinisher ListeningExecutorService finisherExecutorService,
                           final OperationsService operationsService) {
        super(finisherExecutorService, operationsService);
    }

    @Override
    public DownloadUnit constructUnitToSubmit(final Restorer restorer,
                                              final ManifestEntry manifestEntry,
                                              final AtomicBoolean shouldCancel,
                                              final String snapshotTag) {
        return new DownloadUnit(restorer, manifestEntry, shouldCancel, snapshotTag);
    }

    @Override
    public Session<DownloadUnit> constructSession() {
        return new DownloadSession();
    }

    @Override
    public Session<DownloadUnit> submit(final Restorer restorer,
                                        final Operation<? extends BaseRestoreOperationRequest> operation,
                                        final Collection<ManifestEntry> entries,
                                        final String snapshotTag,
                                        final int concurrentConnections) {
        return super.submit(restorer,
                            operation,
                            entries,
                            snapshotTag,
                            concurrentConnections);
    }

    public static class DownloadSession extends AbstractTracker.Session<DownloadUnit> {

    }

    public static class DownloadUnit extends AbstractTracker.Unit {

        @JsonIgnore
        private final Restorer restorer;

        public DownloadUnit(final Restorer restorer,
                            final ManifestEntry manifestEntry,
                            final AtomicBoolean shouldCancel,
                            final String snapshotTag) {
            super(manifestEntry, shouldCancel);
            this.restorer = restorer;
            super.snapshotTag = snapshotTag;
        }

        @Override
        public Void call() {

            state = RUNNING;

            RemoteObjectReference remoteObjectReference = null;
            try {
                remoteObjectReference = restorer.objectKeyToNodeAwareRemoteReference(manifestEntry.objectKey);

                logger.info(String.format("Downloading file %s to %s.", remoteObjectReference.getObjectKey(), manifestEntry.localFile));

                Path localPath = manifestEntry.localFile;

                if (remoteObjectReference.canonicalPath.endsWith("-schema.cql")) {
                    localPath = manifestEntry.localFile.getParent().resolve("schema.cql");
                }

                restorer.downloadFile(localPath, remoteObjectReference);

                logger.info(String.format("Successfully downloaded file %s to %s.", remoteObjectReference.getObjectKey(), localPath));

                state = FINISHED;

                return null;
            } catch (final Throwable t) {
                if (remoteObjectReference != null) {
                    logger.error(String.format("Failed to download file %s.", remoteObjectReference.getObjectKey()), t);
                }

                state = FAILED;
            }

            return null;
        }
    }
}
