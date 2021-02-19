package com.instaclustr.esop.impl.restore;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageInteractor;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;

public abstract class Restorer extends StorageInteractor {

    protected final BaseRestoreOperationRequest request;

    public Restorer(final BaseRestoreOperationRequest request) {
        super(request.storageLocation);
        this.request = request;
    }

    public abstract String downloadFileToString(final RemoteObjectReference objectReference) throws Exception;

    public abstract void downloadFile(final Path localPath, final RemoteObjectReference objectReference) throws Exception;

    public abstract String downloadFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract String downloadManifestToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract String downloadNodeFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract Path downloadNodeFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception;

    // currently works for file protocol only
    public List<Manifest> list() throws Exception {
        throw new UnsupportedOperationException();
    }

    // currently works for file protocol only
    public void delete(final Path objectKey) throws Exception {
        throw new UnsupportedOperationException();
    }

    // currently works for file protocol only
    public void delete(final ManifestReport report, final RemoveBackupRequest request) throws Exception {
        throw new UnsupportedOperationException();
    }
}
