package com.instaclustr.esop.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class StorageInteractor implements AutoCloseable {

    protected StorageLocation storageLocation;

    public abstract RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception;

    public abstract RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws Exception;

    public StorageInteractor(final StorageLocation storageLocation) {
        this.storageLocation = storageLocation;
    }

    public String resolveNodeAwareRemotePath(final Path objectKey) {
        return Paths.get(storageLocation.clusterId).resolve(storageLocation.datacenterId).resolve(storageLocation.nodeId).resolve(objectKey).toString();
    }

    public void updateStorageLocation(final StorageLocation storageLocation) {
        this.storageLocation = storageLocation;
    }

    protected abstract void cleanup() throws Exception;

    private boolean isClosed = false;

    public void close() throws IOException {
        if (isClosed) {
            return;
        }

        try {
            cleanup();

            isClosed = true;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
