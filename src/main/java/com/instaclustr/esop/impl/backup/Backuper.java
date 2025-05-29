package com.instaclustr.esop.impl.backup;

import java.io.InputStream;

import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageInteractor;
import com.instaclustr.esop.impl.retry.Retrier;
import com.instaclustr.esop.impl.retry.RetrierFactory;

public abstract class Backuper extends StorageInteractor {

    protected final BaseBackupOperationRequest request;
    protected final Retrier retrier;

    protected Backuper(final BaseBackupOperationRequest request) {
        super(request.storageLocation);
        this.request = request;
        this.retrier = RetrierFactory.getRetrier(request.retry);
    }

    public static class RefreshingOutcome
    {
        public FreshenResult result;
        public String hash;

        public RefreshingOutcome(FreshenResult result, String hash)
        {
            this.result = result;
            this.hash = hash;
        }
    }

    public enum FreshenResult {
        FRESHENED,
        UPLOAD_REQUIRED
    }

    public abstract RefreshingOutcome freshenRemoteObject(ManifestEntry manifestEntry, final RemoteObjectReference object) throws Exception;

    public abstract void uploadFile(final ManifestEntry manifestEntry,
                                    final InputStream localFileStream,
                                    final RemoteObjectReference objectReference) throws Exception;

    public abstract void uploadText(final String text, final RemoteObjectReference objectReference) throws Exception;

    public void uploadEncryptedFile(final ManifestEntry manifestEntry,
                                    final InputStream localFileStream,
                                    final RemoteObjectReference objectReference) throws Exception {
        uploadFile(manifestEntry, localFileStream, objectReference);
    }

    public void uploadEncryptedText(final String plainText, final RemoteObjectReference objectReference) throws Exception {
        uploadText(plainText, objectReference);
    }
}
