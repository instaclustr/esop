package com.instaclustr.esop.impl.backup;

import java.io.InputStream;

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

    public enum FreshenResult {
        FRESHENED,
        UPLOAD_REQUIRED
    }

    public abstract FreshenResult freshenRemoteObject(final RemoteObjectReference object) throws Exception;

    public abstract void uploadFile(final long size,
                                    final InputStream localFileStream,
                                    final RemoteObjectReference objectReference) throws Exception;

    public abstract void uploadText(final String text, final RemoteObjectReference objectReference) throws Exception;

    public void uploadEncryptedFile(final long size,
                                    final InputStream localFileStream,
                                    final RemoteObjectReference objectReference) throws Exception {
        uploadFile(size, localFileStream, objectReference);
    }

    public void uploadEncryptedText(final String plainText, final RemoteObjectReference objectReference) throws Exception {
        uploadText(plainText, objectReference);
    }
}
