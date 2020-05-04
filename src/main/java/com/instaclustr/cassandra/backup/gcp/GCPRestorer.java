package com.instaclustr.cassandra.backup.gcp;

import static java.lang.String.format;

import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.io.CharStreams;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;

public class GCPRestorer extends Restorer {

    private final Storage storage;

    @AssistedInject
    public GCPRestorer(final GoogleStorageFactory storageFactory,
                       final ExecutorServiceSupplier executorServiceSupplier,
                       @Assisted final RestoreOperationRequest request) {
        super(request, executorServiceSupplier);
        this.storage = storageFactory.build(request);
    }

    @AssistedInject
    public GCPRestorer(final GoogleStorageFactory storageFactory,
                       final ExecutorServiceSupplier executorServiceSupplier,
                       @Assisted final RestoreCommitLogsOperationRequest request) {
        super(request, executorServiceSupplier);
        this.storage = storageFactory.build(request);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) {
        // objectKey is kept simple (e.g. "manifests/autosnap-123") so that it directly reflects the local path
        return new GCPRemoteObjectReference(objectKey, resolveRemotePath(objectKey), request.storageLocation.bucket);
    }

    @Override
    public String downloadFileToString(final Path localFile, final RemoteObjectReference objectReference) throws Exception {
        final BlobId blobId = ((GCPRemoteObjectReference) objectReference).blobId;
        Files.createDirectories(localFile.getParent());

        try (final ReadChannel inputChannel = storage.reader(blobId)) {
            try (final InputStreamReader isr = new InputStreamReader(Channels.newInputStream(inputChannel))) {
                return CharStreams.toString(isr);
            }
        }
    }

    @Override
    public void downloadFile(final Path localFile, final RemoteObjectReference objectReference) throws Exception {
        final BlobId blobId = ((GCPRemoteObjectReference) objectReference).blobId;
        Files.createDirectories(localFile.getParent());

        try (final ReadChannel inputChannel = storage.reader(blobId)) {
            Files.copy(Channels.newInputStream(inputChannel), localFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public Path downloadFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {

        final Page<Blob> blobs = list(request.storageLocation.bucket, remotePrefix.toString());
        final List<Blob> blobItems = new ArrayList<>();

        for (final Blob blob : blobs.iterateAll()) {
            if (keyFilter.test(blob.getName())) {
                blobItems.add(blob);
            }
        }

        if (blobItems.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter: %s", blobItems.toString()));
        }

        final String blobItemPath = blobItems.get(0).getName();
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];

        final Path destination = destinationDir.resolve(fileName);

        downloadFile(destination, objectKeyToRemoteReference(remotePrefix.resolve(fileName)));

        return destination;
    }

    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) {
        final GCPRemoteObjectReference gcpRemoteObjectReference = (GCPRemoteObjectReference) prefix;

        final String bucket = gcpRemoteObjectReference.blobId.getBucket();
        final String pathPrefix = gcpRemoteObjectReference.getObjectKey().toString();

        list(bucket, pathPrefix).iterateAll().iterator().forEachRemaining(blob -> {
            if (!blob.getName().endsWith("/")) {
                consumer.accept(objectKeyToRemoteReference(removeNodePrefix(blob)));
            }
        });
    }

    private Path removeNodePrefix(final Blob blob) {
        final Pattern nodeIdPattern = Pattern.compile(request.storageLocation.clusterId + "/" + request.storageLocation.datacenterId + "/" + request.storageLocation.nodeId + "/");
        return Paths.get(nodeIdPattern.matcher(blob.getName()).replaceFirst(""));
    }

    private Page<Blob> list(final String bucket, final String pathPrefix) {
        return storage.list(bucket, BlobListOption.prefix(request.storageLocation.clusterId
                                                              + "/" + request.storageLocation.datacenterId
                                                              + "/" + request.storageLocation.nodeId
                                                              + "/" + pathPrefix + "/"),
                            BlobListOption.currentDirectory());
    }

    @Override
    public void cleanup() throws Exception {
        // Nothing to cleanup
    }
}
