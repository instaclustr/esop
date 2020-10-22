package com.instaclustr.esop.gcp;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
import java.util.stream.Collectors;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.io.CharStreams;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class GCPRestorer extends Restorer {

    private final Storage storage;

    @AssistedInject
    public GCPRestorer(final GoogleStorageFactory storageFactory,
                       @Assisted final RestoreOperationRequest request) {
        super(request);
        this.storage = storageFactory.build(request);
    }

    @AssistedInject
    public GCPRestorer(final GoogleStorageFactory storageFactory,
                       @Assisted final RestoreCommitLogsOperationRequest request) {
        super(request);
        this.storage = storageFactory.build(request);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        return new GCPRemoteObjectReference(objectKey, objectKey.toString(), request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) {
        // objectKey is kept simple (e.g. "manifests/autosnap-123") so that it directly reflects the local path
        return new GCPRemoteObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey), request.storageLocation.bucket);
    }

    @Override
    public String downloadFileToString(final RemoteObjectReference objectReference) throws Exception {
        final BlobId blobId = ((GCPRemoteObjectReference) objectReference).blobId;

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
    public String downloadFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {

        // special case for GCP, here we take prefix as a parent dir of "remotePrefix" as it lists just these files from there

        String resolvedPrefix = remotePrefix.toString();

        if (remotePrefix.getParent() != null) {
            // slash at the end seems to be important
            resolvedPrefix = remotePrefix.getParent().toString() + "/";
        }

        final String blobItemPath = getBlobItemPath(list(request.storageLocation.bucket, resolvedPrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];
        return downloadFileToString(objectKeyToRemoteReference(Paths.get(resolvedPrefix).resolve(fileName)));
    }

    @Override
    public String downloadManifestToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getManifest(nodeList(request.storageLocation.bucket, remotePrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public String downloadNodeFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getBlobItemPath(nodeList(request.storageLocation.bucket, remotePrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public Path downloadNodeFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getBlobItemPath(nodeList(request.storageLocation.bucket, remotePrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];

        final Path destination = destinationDir.resolve(fileName);

        downloadFile(destination, objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));

        return destination;
    }

    private String getManifest(final Page<Blob> blobItemsIterable, final Predicate<String> keyFilter) {

        final List<Blob> manifests = new ArrayList<>();

        for (final Blob blob : blobItemsIterable.iterateAll()) {
            if (keyFilter.test(blob.getName())) {
                manifests.add(blob);
            }
        }

        if (manifests.isEmpty()) {
            throw new IllegalStateException("There is no manifest requested found.");
        }

        return Manifest.parseLatestManifest(manifests.stream().map(BlobInfo::getName).collect(toList()));
    }

    private String getBlobItemPath(final Page<Blob> blobs, final Predicate<String> keyFilter) {
        final List<Blob> blobItems = new ArrayList<>();

        for (final Blob blob : blobs.iterateAll()) {
            if (keyFilter.test(blob.getName())) {
                blobItems.add(blob);
            }
        }

        if (blobItems.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter: %s", blobItems.toString()));
        }

        return blobItems.get(0).getName();
    }

    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) {
        final GCPRemoteObjectReference gcpRemoteObjectReference = (GCPRemoteObjectReference) prefix;

        final String bucket = gcpRemoteObjectReference.blobId.getBucket();
        final String pathPrefix = gcpRemoteObjectReference.getObjectKey().toString();

        nodeList(bucket, Paths.get(pathPrefix)).iterateAll().iterator().forEachRemaining(blob -> {
            if (!blob.getName().endsWith("/")) {
                consumer.accept(objectKeyToNodeAwareRemoteReference(removeNodePrefix(blob)));
            }
        });
    }

    private Path removeNodePrefix(final Blob blob) {
        final String pattern = String.format("%s/%s/%s/",
                                             request.storageLocation.clusterId,
                                             request.storageLocation.datacenterId,
                                             request.storageLocation.nodeId);
        final Pattern nodeIdPattern = Pattern.compile(pattern);
        return Paths.get(nodeIdPattern.matcher(blob.getName()).replaceFirst(""));
    }

    private Page<Blob> globalList(final String bucket, final Path pathPrefix) {
        return list(bucket, pathPrefix.toString());
    }

    private Page<Blob> nodeList(final String bucket, final Path prefix) {
        final String resolvedPrefix = String.format("%s/%s/%s/%s/",
                                                    request.storageLocation.clusterId,
                                                    request.storageLocation.datacenterId,
                                                    request.storageLocation.nodeId,
                                                    prefix.toString());

        return list(bucket, resolvedPrefix);
    }

    private Page<Blob> list(final String bucket, final String prefix) {
        final String resolvedPrefix = prefix.startsWith("/") ? prefix.replaceFirst("/", "") : prefix;
        return storage.list(bucket, BlobListOption.prefix(resolvedPrefix), BlobListOption.currentDirectory());
    }

    @Override
    public void cleanup() throws Exception {
        // Nothing to cleanup
    }
}
