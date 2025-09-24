package com.instaclustr.esop.impl.restore;

import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageInteractor;

public abstract class Restorer extends StorageInteractor {

    protected final BaseRestoreOperationRequest request;

    public Restorer(final BaseRestoreOperationRequest request) {
        super(request.storageLocation);
        this.request = request;
    }

    public String downloadFileToString(final RemoteObjectReference objectReference, boolean isEncrypted) throws Exception {
        return downloadFileToString(objectReference);
    }

    public abstract String downloadFileToString(final RemoteObjectReference objectReference) throws Exception;

    public abstract void downloadFile(final Path localPath, ManifestEntry manifestEntry, final RemoteObjectReference objectReference) throws Exception;

    public void downloadFile(final Path localPath, final RemoteObjectReference objectReference) throws Exception {
        downloadFile(localPath, null, objectReference);
    }

    // topologies are always not encrypted
    public abstract String downloadTopology(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    // manifests are always not encrypted
    public abstract String downloadManifest(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    // currently used only in tests
    public abstract String downloadNodeFile(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception;
}
