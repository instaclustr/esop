package com.instaclustr.esop.s3.v2;

import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.s3.v1.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;

public class BaseS3Restorer extends Restorer
{
    private final S3Clients s3Clients;

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          RestoreOperationRequest request)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request);
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          RestoreCommitLogsOperationRequest request)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request);
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          ObjectMapper objectMapper,
                          ListOperationRequest request)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request);
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          ObjectMapper objectMapper,
                          RemoveBackupRequest request)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) {
        return new S3RemoteObjectReference(objectKey, objectKey.toFile().toString());
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) {
        return new S3RemoteObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey));
    }

    @Override
    protected void cleanup() throws Exception {
        s3Clients.close();
    }

    @Override
    public String downloadFileToString(RemoteObjectReference objectReference) throws Exception {
        return null;
    }

    @Override
    public void downloadFile(Path localPath, RemoteObjectReference objectReference) throws Exception {

    }

    @Override
    public String downloadFileToString(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        return null;
    }

    @Override
    public String downloadManifestToString(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        return null;
    }

    @Override
    public String downloadNodeFileToString(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        return null;
    }

    @Override
    public Path downloadNodeFileToDir(Path destinationDir, Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        return null;
    }

    @Override
    public void consumeFiles(RemoteObjectReference prefix, Consumer<RemoteObjectReference> consumer) throws Exception {

    }
}
