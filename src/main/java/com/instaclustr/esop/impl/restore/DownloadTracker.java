package com.instaclustr.esop.impl.restore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ListeningExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.inject.Inject;
import com.instaclustr.esop.impl.AbstractTracker;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.ManifestEntry.Type;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.hash.HashService.HashVerificationException;
import com.instaclustr.esop.impl.hash.HashServiceImpl;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.restore.DownloadTracker.DownloadSession;
import com.instaclustr.esop.impl.restore.DownloadTracker.DownloadUnit;
import com.instaclustr.esop.impl.restore.RestoreModules.DownloadingFinisher;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;

import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.FAILED;
import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.FINISHED;
import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.RUNNING;

public class DownloadTracker extends AbstractTracker<DownloadUnit, DownloadSession, Restorer, BaseRestoreOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(DownloadTracker.class);

    @Inject
    public DownloadTracker(final @DownloadingFinisher ListeningExecutorService finisherExecutorService,
                           final OperationsService operationsService,
                           final HashSpec hashSpec) {
        super(finisherExecutorService, operationsService, hashSpec);
    }

    @Override
    public DownloadUnit constructUnitToSubmit(final Restorer restorer,
                                              final ManifestEntry manifestEntry,
                                              final AtomicBoolean shouldCancel,
                                              final String snapshotTag,
                                              final HashSpec hashSpec) {
        return new DownloadUnit(restorer, manifestEntry, shouldCancel, snapshotTag, hashSpec);
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
                            final String snapshotTag,
                            final HashSpec hashSpec) {
            super(manifestEntry, shouldCancel, hashSpec);
            this.restorer = restorer;
            super.snapshotTag = snapshotTag;
        }

        @Override
        public Void call() {
            try {
                state = RUNNING;
                RemoteObjectReference remoteObjectReference = restorer.objectKeyToNodeAwareRemoteReference(manifestEntry.objectKey);

                Path localPath = manifestEntry.localFile;

                if (remoteObjectReference.canonicalPath.endsWith("-schema.cql")) {
                    localPath = manifestEntry.localFile.getParent().resolve("schema.cql");
                }

                if (!Files.exists(localPath)) {
                    logger.debug(String.format("Downloading file %s to %s.", remoteObjectReference.getObjectKey(), manifestEntry.localFile));

                    restorer.downloadFile(localPath, manifestEntry, remoteObjectReference);

                    // hash upon downloading
                    try {
                        if (manifestEntry.type == Type.FILE) {
                            new HashServiceImpl(hashSpec).verify(localPath, manifestEntry.hash);
                        }
                    } catch (final HashVerificationException ex) {
                        // delete it if has is wrong so on the next try, it will be missing and we will download it again
                        Files.deleteIfExists(localPath);
                        throw ex;
                    }

                    logger.debug(String.format("Successfully downloaded file %s to %s.", remoteObjectReference.getObjectKey(), localPath));

                    state = FINISHED;

                    return null;
                } else if (manifestEntry.hash != null) {
                    logger.info(String.format("Skipping download of file %s to %s, file already exists locally.",
                                              remoteObjectReference.getObjectKey(), manifestEntry.localFile));
                    // if it exists, verify its hash to be sure it was not altered
                    new HashServiceImpl(hashSpec).verify(localPath, manifestEntry.hash);
                    state = FINISHED;
                } else {
                    // if it exists and manifest does not have hash field, consider it to be finished without any check
                    state = FINISHED;
                }
            } catch (final Throwable t) {
                state = FAILED;
                logger.error(String.format("Failed to download file %s", manifestEntry.localFile), t.getMessage());
                throwable = t;
            }

            return null;
        }
    }
}
