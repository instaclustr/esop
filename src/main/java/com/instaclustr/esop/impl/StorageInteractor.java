package com.instaclustr.esop.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;

public abstract class StorageInteractor implements AutoCloseable {

    protected StorageLocation storageLocation;

    public abstract RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception;

    public abstract RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws Exception;

    public Path resolveRoot() {
        return Paths.get("/");
    }

    public StorageInteractor(final StorageLocation storageLocation) {
        this.storageLocation = storageLocation;
    }

    public String resolveNodeAwareRemotePath(final Path objectKey) {
        return Paths.get(storageLocation.clusterId).resolve(storageLocation.datacenterId).resolve(storageLocation.nodeId).resolve(objectKey).toString();
    }

    // currently works for file protocol only
    public List<Manifest> listManifests() throws Exception {
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

    // currently works for file protocol only
    public List<StorageLocation> listNodes() throws Exception {
        throw new UnsupportedOperationException();
    }

    // currently works for file protocol only
    public List<StorageLocation> listNodes(final String dc) throws Exception {
        throw new UnsupportedOperationException();
    }

    // currently works for file protocol only
    public List<StorageLocation> listNodes(final List<String> dcs) throws Exception {
        throw new UnsupportedOperationException();
    }

    // currently works for file protocol only
    public List<String> listDcs() throws Exception {
        throw new UnsupportedOperationException();
    }

    // currently works for file protocol only
    public void deleteTopology(final String name) throws Exception {
        delete(resolveRoot().resolve("topology").resolve(name + ".json"));
    }

    public StorageLocation getStorageLocation() {
        return this.storageLocation;
    }

    public void setStorageLocation(final StorageLocation storageLocation) {
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
