package com.instaclustr.cassandra.backup.impl.restore;

import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.StorageInteractor;

public abstract class Restorer extends StorageInteractor {

    protected final BaseRestoreOperationRequest request;

    public Restorer(final BaseRestoreOperationRequest request) {
        super(request.storageLocation);
        this.request = request;
    }

    public abstract String downloadFileToString(final RemoteObjectReference objectReference) throws Exception;

    public abstract void downloadFile(final Path localPath, final RemoteObjectReference objectReference) throws Exception;

    public abstract String downloadFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract String downloadNodeFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract Path downloadNodeFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception;

    public abstract void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception;
}
