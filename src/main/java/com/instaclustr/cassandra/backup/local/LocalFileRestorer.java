package com.instaclustr.cassandra.backup.local;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;

public class LocalFileRestorer extends Restorer {

    @AssistedInject
    public LocalFileRestorer(final ExecutorServiceSupplier executorServiceSupplier,
                             @Assisted final RestoreOperationRequest request) {
        super(request, executorServiceSupplier);
    }

    @AssistedInject
    public LocalFileRestorer(final ExecutorServiceSupplier executorServiceSupplier,
                             @Assisted final RestoreCommitLogsOperationRequest request) {
        super(request, executorServiceSupplier);
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws Exception {
        return new LocalFileObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey));
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        return new LocalFileObjectReference(objectKey, objectKey.toFile().getCanonicalFile().toString());
    }

    @Override
    public String downloadFileToString(final RemoteObjectReference objectReference) throws Exception {
        final Path remoteFilePath = request.storageLocation.fileBackupDirectory
            .resolve(request.storageLocation.bucket)
            .resolve(Paths.get(((LocalFileObjectReference) objectReference).canonicalPath));

        return new String(Files.readAllBytes(remoteFilePath), StandardCharsets.UTF_8);
    }

    @Override
    public void downloadFile(final Path localFilePath, final RemoteObjectReference objectReference) throws Exception {
        final Path remoteFilePath = request.storageLocation.fileBackupDirectory
            .resolve(request.storageLocation.bucket)
            .resolve(Paths.get(((LocalFileObjectReference) objectReference).canonicalPath));

        //Assume that any path passed in to this function is a file
        Files.createDirectories(localFilePath.getParent());

        Files.copy(remoteFilePath, localFilePath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public String downloadFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItem = getFileToDownload(keyFilter, remotePrefix);
        return new String(Files.readAllBytes(Paths.get(blobItem)));
    }

    @Override
    public Path downloadFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String fileName = getFileToDownload(keyFilter, remotePrefix);
        final Path destination = destinationDir.resolve(fileName);
        downloadFile(destination, objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
        return destination;
    }

    private String getFileToDownload(final Predicate<String> keyFilter, final Path remotePrefix) throws Exception {

        final Path pathToList = Paths.get(request.storageLocation.rawLocation.replaceAll("file://", "")).resolve(remotePrefix);

        System.out.println(pathToList);

        final List<Path> filtered = Files.list(pathToList)
            .filter(path -> !Files.isDirectory(path) && keyFilter.test(path.toString()))
            .collect(toList());

        if (filtered.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter! %s for remote prefix %s",
                                                   filtered.toString(),
                                                   remotePrefix));
        }
        return filtered.get(0).getFileName().toString();
    }

    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception {

        final Path directoryToWalk = request.storageLocation.fileBackupDirectory.resolve(request.storageLocation.bucket).resolve(prefix.canonicalPath);

        if (!Files.exists(directoryToWalk)) {
            return;
        }

        final List<Path> pathsList = Files.walk(directoryToWalk)
            .filter(filePath -> Files.isRegularFile(filePath))
            .collect(toList());

        for (final Path path : pathsList) {
            consumer.accept(objectKeyToNodeAwareRemoteReference(path));
        }
    }

    @Override
    public void cleanup() {
        // Nothing to cleanup
    }
}
