package com.instaclustr.esop.s3.v2;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.io.CharStreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.Manifest;
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
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;

import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;

public class BaseS3Restorer extends Restorer
{
    private static final Logger logger = LoggerFactory.getLogger(BaseS3Restorer.class);

    private final S3Clients s3Clients;
    private S3TransferManager nonEncryptingTransferManager;
    private Optional<S3TransferManager> encryptingTransferManager;

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          RestoreOperationRequest request,
                          S3ConfigurationResolver configurationResolver)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
        prepareTransferManager();
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          RestoreCommitLogsOperationRequest request,
                          S3ConfigurationResolver configurationResolver)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
        prepareTransferManager();
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          ListOperationRequest request,
                          S3ConfigurationResolver configurationResolver)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
        prepareTransferManager();
    }

    public BaseS3Restorer(S3ClientsFactory s3ClientsFactory,
                          RemoveBackupRequest request,
                          S3ConfigurationResolver configurationResolver)
    {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
        prepareTransferManager();
    }

    private void prepareTransferManager() {
        nonEncryptingTransferManager = S3TransferManager.builder()
                                                        .s3Client(s3Clients.getNonEncryptingClient())
                                                        .build();

        encryptingTransferManager = s3Clients.getEncryptingClient()
                                             .map(c -> S3TransferManager.builder().s3Client(c).build());
    }

    private class DownloadTransferListener implements TransferListener {

        private final String key;
        public DownloadTransferListener(String key) {
            this.key = key;
        }

        @Override
        public void transferInitiated(Context.TransferInitiated context) {
            logger.info("Downloading " + key);
        }

        @Override
        public void transferComplete(Context.TransferComplete context) {
            logger.info("Finished downloading " + key);
        }

        @Override
        public void transferFailed(Context.TransferFailed context) {
            logger.error("Failed to download " + key, context.exception().getMessage());
        }
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
        S3TransferManager s3TransferManager = encryptingTransferManager.orElse(nonEncryptingTransferManager);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                            .bucket(request.storageLocation.bucket)
                                                            .key(objectReference.canonicalPath)
                                                            .build();

        CompletedDownload<ResponseBytes<GetObjectResponse>> response = s3TransferManager
        .download(DownloadRequest.builder()
                                 .getObjectRequest(getObjectRequest)
                                 .responseTransformer(AsyncResponseTransformer.toBytes())
                                 .addTransferListener(new DownloadTransferListener(objectReference.canonicalPath))
                                 .build())
        .completionFuture().get();

        try (final InputStream is = response.result().asInputStream();
             final InputStreamReader isr = new InputStreamReader(is)) {
            return CharStreams.toString(isr);
        }
    }


    @Override
    public void downloadFile(Path localPath, RemoteObjectReference objectReference) throws Exception {
        S3TransferManager s3TransferManager = encryptingTransferManager.orElse(nonEncryptingTransferManager);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                            .bucket(request.storageLocation.bucket)
                                                            .key(objectReference.canonicalPath)
                                                            .build();

        s3TransferManager.download(DownloadRequest.builder()
                                                  .getObjectRequest(getObjectRequest)
                                                  .responseTransformer(AsyncResponseTransformer.toFile(localPath))
                                                  .addTransferListener(new DownloadTransferListener(objectReference.canonicalPath))
                                                  .build())
                         .completionFuture().get();
    }

    @Override
    public String downloadFileToString(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        S3Object s3Object = getBlobItemPath(remotePrefix.toString(), keyFilter);
        return downloadFileToString(objectKeyToRemoteReference(Paths.get(s3Object.key())));
    }

    @Override
    public String downloadManifestToString(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        final S3Object manifestObject = getManifest(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = manifestObject.key().split("/")[manifestObject.key().split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public String downloadNodeFileToString(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = s3Object.key().split("/")[s3Object.key().split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public Path downloadNodeFileToDir(Path destinationDir, Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = s3Object.key().split("/")[s3Object.key().split("/").length - 1];
        final Path destination = destinationDir.resolve(fileName);

        downloadFile(destination, objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));

        return destination;
    }

    @Override
    public void consumeFiles(RemoteObjectReference prefix, Consumer<RemoteObjectReference> consumer) throws Exception {
        final Path bucketPath = Paths.get(request.storageLocation.clusterId).resolve(request.storageLocation.datacenterId).resolve(request.storageLocation.nodeId);
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                                                                        .bucket(request.storageLocation.bucket)
                                                                        .prefix(prefix.canonicalPath)
                                                                        .build();

        ListObjectsV2Response listObjectsV2Response;

        final List<S3Object> summaryList = new ArrayList<>();

        do {
            listObjectsV2Response = s3Clients.getClient().listObjectsV2(listObjectsV2Request).get();
            listObjectsV2Response.contents().stream()
                                 .filter(o -> !o.key().endsWith("/"))
                                 .forEach(o -> consumer.accept(objectKeyToNodeAwareRemoteReference(bucketPath.relativize(Paths.get(o.key())))));
        } while (listObjectsV2Response.isTruncated());
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

    private List<S3Object> listBucket(final String remotePrefix, final Predicate<String> keyFilter) throws Exception {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                                                                        .bucket(request.storageLocation.bucket)
                                                                        .prefix(remotePrefix)
                                                                        .build();

        ListObjectsV2Response listObjectsV2Response;

        final List<S3Object> summaryList = new ArrayList<>();

        do {
            listObjectsV2Response = s3Clients.getClient().listObjectsV2(listObjectsV2Request).get();
            listObjectsV2Response.contents().stream()
                                 .filter(o -> !o.key().endsWith("/"))
                                 .filter(o -> keyFilter.test(o.key()))
                                 .collect(toCollection(() -> summaryList));
        } while (listObjectsV2Response.isTruncated());

        return summaryList;
    }

    private S3Object getBlobItemPath(final String remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final List<S3Object> summaryList = listBucket(remotePrefix, keyFilter);

        if (summaryList.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter: %s", summaryList));
        }

        return summaryList.get(0);
    }

    private S3Object getManifest(final String remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final List<S3Object> summaryList = listBucket(remotePrefix, keyFilter);

        if (summaryList.isEmpty()) {
            throw new IllegalStateException("There is no manifest requested found.");
        }

        final String manifestFullKey = Manifest.parseLatestManifest(summaryList.stream().map(S3Object::key).collect(Collectors.toList()));

        return summaryList.stream()
                          .filter(o -> o.key().endsWith(manifestFullKey))
                          .findFirst()
                          .orElseThrow(() -> new IllegalStateException("Unable to get the latest manifest from remote prefix " + remotePrefix));
    }
}
