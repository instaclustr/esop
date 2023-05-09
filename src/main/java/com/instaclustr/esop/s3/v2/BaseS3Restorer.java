package com.instaclustr.esop.s3.v2;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.v1.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;

public class BaseS3Restorer extends Restorer
{
    private final S3Clients s3Clients;

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          RestoreOperationRequest request,
                          S3ConfigurationResolver configurationResolver)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          RestoreCommitLogsOperationRequest request,
                          S3ConfigurationResolver configurationResolver)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          ListOperationRequest request,
                          S3ConfigurationResolver configurationResolver)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          RemoveBackupRequest request,
                          S3ConfigurationResolver configurationResolver)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
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

    @Override
    public List<StorageLocation> listNodes() throws Exception {
        return localFileRestorer.listNodes();
    }

    @Override
    public List<StorageLocation> listNodes(final String dc) throws Exception {
        return localFileRestorer.listNodes(dc);
    }

    @Override
    public List<StorageLocation> listNodes(final List<String> dcs) throws Exception {
        return localFileRestorer.listNodes(dcs);
    }

    @Override
    public List<String> listDcs() throws Exception {
        return localFileRestorer.listDcs();
    }
}
