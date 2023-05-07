package com.instaclustr.esop.s3.v2;

import java.io.InputStream;
import java.nio.file.Path;

import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.s3.v1.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;

public class BaseS3Backuper extends Backuper {
    private final S3Clients s3Clients;

    public BaseS3Backuper(final S3ClientsFactory s3ClientsFactory,
                          final BackupOperationRequest request) {
        super(request);
        s3Clients = s3ClientsFactory.build(request);
    }

    public BaseS3Backuper(final S3ClientsFactory s3ClientsFactory,
                          final BackupCommitLogsOperationRequest request) {
        super(request);
        s3Clients = s3ClientsFactory.build(request);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) {
        return new S3RemoteObjectReference(objectKey, objectKey.toString());
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
    public FreshenResult freshenRemoteObject(RemoteObjectReference object) throws Exception {
        return null;
    }

    @Override
    public void uploadFile(long size, InputStream localFileStream, RemoteObjectReference objectReference) throws Exception {

    }

    @Override
    public void uploadText(String text, RemoteObjectReference objectReference) throws Exception {

    }
}
