package com.instaclustr.cassandra.backup.local;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.Backuper;

public class LocalFileBackuper extends Backuper {

    @AssistedInject
    public LocalFileBackuper(@Assisted final BackupOperationRequest request) {
        super(request);
    }

    @AssistedInject
    public LocalFileBackuper(@Assisted final BackupCommitLogsOperationRequest request) {
        super(request);
    }

    private Path resolveFullRemoteObjectPath(final RemoteObjectReference objectReference) {
        return request.storageLocation.fileBackupDirectory.resolve(request.storageLocation.bucket).resolve(objectReference.canonicalPath);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        return new LocalFileObjectReference(objectKey, objectKey.toFile().getCanonicalFile().toString());
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws Exception {
        return new LocalFileObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey));
    }

    @Override
    public FreshenResult freshenRemoteObject(final RemoteObjectReference object) throws Exception {
        final File fullRemoteObject = resolveFullRemoteObjectPath(object).toFile();
        if (fullRemoteObject.exists()) {
            //if we can't update modified time for whatever reason, then we will re-upload
            if (fullRemoteObject.setLastModified(System.currentTimeMillis())) {
                return FreshenResult.FRESHENED;
            }
        }
        return FreshenResult.UPLOAD_REQUIRED;
    }

    @Override
    public void uploadFile(final long size,
                           final InputStream localFileStream,
                           final RemoteObjectReference objectReference) throws Exception {
        Path remotePath = resolveFullRemoteObjectPath(objectReference);
        Files.createDirectories(remotePath.getParent());
        Files.copy(localFileStream, remotePath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void uploadText(final String text, final RemoteObjectReference objectReference) throws Exception {
        Path dir = request.storageLocation.fileBackupDirectory.resolve(request.storageLocation.bucket);
        Files.createDirectories(dir.resolve(objectReference.objectKey).getParent());
        Files.write(dir.resolve(objectReference.objectKey), text.getBytes());
    }

    @Override
    public void cleanup() throws Exception {
        //No clean up required
    }
}
