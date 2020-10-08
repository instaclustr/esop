package com.instaclustr.esop.impl;

import java.nio.file.Path;

import com.instaclustr.esop.impl.restore.RestoreOperationRequest;

public class Directories {

    private final RestoreOperationRequest request;

    public Directories(final RestoreOperationRequest request) {
        this.request = request;
    }

    public Path data() {
        return request.cassandraDirectory.resolve("data");
    }

    public Path hints() {
        return request.cassandraDirectory.resolve("hint");
    }

    public Path savedCaches() {
        return request.cassandraDirectory.resolve("saved_caches");
    }

    public Path commitLogs() {
        return request.cassandraDirectory.resolve("commitlogs");
    }

    public boolean dataDirExists() {
        return data().toFile().exists();
    }

    public boolean hintsDirExists() {
        return hints().toFile().exists();
    }

    public boolean savedCachesDirExists() {
        return savedCaches().toFile().exists();
    }

    public boolean commitLogsDirExists() {
        return commitLogs().toFile().exists();
    }
}
