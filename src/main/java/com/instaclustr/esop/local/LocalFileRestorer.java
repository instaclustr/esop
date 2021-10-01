package com.instaclustr.esop.local;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.Manifest.ManifestAgePathComparator;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.ManifestEntry.Type;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileRestorer extends Restorer {

    private static final Logger logger = LoggerFactory.getLogger(LocalFileRestorer.class);
    private ObjectMapper objectMapper;

    @AssistedInject
    public LocalFileRestorer(@Assisted final RestoreOperationRequest request) {
        super(request);
    }

    @AssistedInject
    public LocalFileRestorer(@Assisted final RestoreCommitLogsOperationRequest request) {
        super(request);
    }

    @AssistedInject
    public LocalFileRestorer(@Assisted final ListOperationRequest request,
                             ObjectMapper objectMapper) {
        super(request);
        this.objectMapper = objectMapper;
    }

    @AssistedInject
    public LocalFileRestorer(@Assisted final RemoveBackupRequest request,
                             ObjectMapper objectMapper) {
        super(request);
        this.objectMapper = objectMapper;
    }

    @Override
    public Path resolveRoot() {
        return request.storageLocation.fileBackupDirectory.resolve(request.storageLocation.bucket);
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
    public String downloadFileToString(final RemoteObjectReference objectReference) throws Exception {
        final Path remoteFilePath = request.storageLocation.fileBackupDirectory
            .resolve(request.storageLocation.bucket)
            .resolve(Paths.get(objectReference.canonicalPath));

        return new String(Files.readAllBytes(remoteFilePath), StandardCharsets.UTF_8);
    }

    @Override
    public void downloadFile(final Path localFilePath, final RemoteObjectReference objectReference) throws Exception {
        final Path remoteFilePath = request.storageLocation.fileBackupDirectory
            .resolve(request.storageLocation.bucket)
            .resolve(Paths.get(objectReference.canonicalPath));

        //Assume that any path passed in to this function is a file
        Files.createDirectories(localFilePath.getParent());

        Files.copy(remoteFilePath, localFilePath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public String downloadFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {

        Path pathToList = request.storageLocation.fileBackupDirectory.resolve(request.storageLocation.bucket);

        if (remotePrefix.getParent() != null) {
            pathToList = pathToList.resolve(remotePrefix.getParent());
        }

        String fileToDownload = getFileToDownload(pathToList,
                                                  keyFilter,
                                                  remotePrefix);

        return new String(Files.readAllBytes(Paths.get(fileToDownload)));
    }

    @Override
    public String downloadManifestToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final Path pathToList = Paths.get(request.storageLocation.rawLocation.replaceAll("file://", "")).resolve(remotePrefix);
        final String blobItem = getManifest(pathToList, keyFilter, remotePrefix);
        return new String(Files.readAllBytes(Paths.get(blobItem)));
    }

    @Override
    public String downloadNodeFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final Path pathToList = Paths.get(request.storageLocation.rawLocation.replaceAll("file://", "")).resolve(remotePrefix);
        final String blobItem = getFileToDownload(pathToList, keyFilter, remotePrefix);
        return new String(Files.readAllBytes(Paths.get(blobItem)));
    }

    @Override
    public Path downloadNodeFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final Path pathToList = Paths.get(request.storageLocation.rawLocation.replaceAll("file://", "")).resolve(remotePrefix);
        final String fileName = getFileToDownload(pathToList, keyFilter, remotePrefix);
        final Path destination = destinationDir.resolve(fileName);
        downloadFile(destination, objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
        return destination;
    }

    private String getManifest(final Path pathToList, final Predicate<String> keyFilter, final Path remotePrefix) throws Exception {
        final List<Path> manifests = Files.list(pathToList)
            .filter(path -> !Files.isDirectory(path) && keyFilter.test(path.toString()))
            .collect(toList());

        if (manifests.isEmpty()) {
            throw new IllegalStateException("There is no manifest requested found.");
        }

        return Manifest.parseLatestManifest(manifests.stream().map(m -> m.toAbsolutePath().toString()).collect(toList()));
    }

    private String getFileToDownload(final Path pathToList, final Predicate<String> keyFilter, final Path remotePrefix) throws Exception {
        final List<Path> filtered = Files.list(pathToList)
            .filter(path -> !Files.isDirectory(path) && keyFilter.test(path.toString()))
            .collect(toList());

        if (filtered.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter! %s for remote prefix %s",
                                                   filtered,
                                                   remotePrefix));
        }
        return filtered.get(0).toString();
    }

    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception {

        final Path directoryToWalk = request.storageLocation.fileBackupDirectory.resolve(request.storageLocation.bucket).resolve(prefix.canonicalPath);

        if (!Files.exists(directoryToWalk)) {
            return;
        }

        final List<Path> pathsList = Files.walk(directoryToWalk)
            .filter(Files::isRegularFile)
            .collect(toList());

        for (final Path path : pathsList) {
            consumer.accept(objectKeyToNodeAwareRemoteReference(path));
        }
    }

    @Override
    public List<Manifest> listManifests() throws Exception {
        assert objectMapper != null;
        final List<Path> manifests = Files.list(Paths.get(storageLocation.rawLocation.replaceAll("file://", ""), "manifests"))
            .sorted(new ManifestAgePathComparator())
            .collect(toList());

        final List<Manifest> manifestsList = new ArrayList<>();

        for (final Path manifest : manifests) {
            final Manifest read = Manifest.read(manifest, objectMapper);
            read.setManifest(new ManifestEntry(Paths.get("manifests", manifest.getFileName().toString()), manifest, Type.FILE, null));
            manifestsList.add(read);
        }

        return manifestsList;
    }

    @Override
    public void delete(final Path objectKey) throws Exception {
        final RemoteObjectReference remoteObjectReference = objectKeyToNodeAwareRemoteReference(objectKey);
        final Path fileToDelete = request.storageLocation.fileBackupDirectory
            .resolve(request.storageLocation.bucket)
            .resolve(remoteObjectReference.canonicalPath);
        logger.info("Deleting file {}", fileToDelete);
        Files.deleteIfExists(fileToDelete);
    }

    @Override
    public void delete(final ManifestReport backupToDelete, final RemoveBackupRequest request) throws Exception {
        logger.info("Deleting backup {}", backupToDelete.name);
        if (backupToDelete.reclaimableSpace > 0 && !backupToDelete.getRemovableEntries().isEmpty()) {
            for (final String removableEntry : backupToDelete.getRemovableEntries()) {
                if (!request.dry) {
                    delete(Paths.get(removableEntry));
                } else {
                    logger.info("Deletion of {} was executed in dry mode.", removableEntry);
                }
            }
        }

        // manifest and topology as the last
        if (!request.dry) {
            delete(backupToDelete.manifest.objectKey);
        } else {
            logger.info("Deletion of manifest {} was executed in dry mode.", backupToDelete.manifest.objectKey);
        }

        if (!request.dry && request.storageLocation.storageProvider.equals("file")) {
            final List<Path> emptySSTableDirectories = getEmptyDirectories(request.storageLocation);

            for (final Path emptySSTableDir : emptySSTableDirectories) {
                logger.info("Deleting empty sstable directory {}", emptySSTableDir.toAbsolutePath());
                if (!request.dry) {
                    Files.delete(emptySSTableDir);
                }
            }

            // by deleting empty dirs after sstables, we might suddently have an empty table dir
            // so run another round to get rid of them too
            final List<Path> emptyTableDirectories = getEmptyDirectories(request.storageLocation);

            for (final Path emptyTableDir : emptyTableDirectories) {
                logger.info("Deleting empty table directory {}", emptyTableDir.toAbsolutePath());
                if (!request.dry) {
                    Files.delete(emptyTableDir);
                }
            }

            // by deleting empty dirs of tables, we might suddenly have an empty keyspace dir
            // so run another round to get rid of them too
            final List<Path> emptyKeyspaceDirectories = getEmptyDirectories(request.storageLocation);

            for (final Path emptyKeyspaceDir : emptyKeyspaceDirectories) {
                logger.info("Deleting empty keyspace directory {}", emptyKeyspaceDir.toAbsolutePath());
                if (!request.dry) {
                    Files.delete(emptyKeyspaceDir);
                }
            }
        }
    }

    @Override
    public List<StorageLocation> listNodes() throws Exception {
        final List<String> dcs = listDcs();
        final List<StorageLocation> locations = new ArrayList<>();

        for (final String dc : dcs) {
            locations.addAll(listNodes(dc));
        }

        return locations;
    }

    @Override
    public List<StorageLocation> listNodes(final String dc) throws Exception {
        return getDirectories(Paths.get(StorageLocation.updateDatacenter(storageLocation, dc).withoutNode().replace("file://", "")))
                .stream()
                .map(p -> new StorageLocation("file://" + p.toAbsolutePath())).collect(toList());
    }

    @Override
    public List<StorageLocation> listNodes(final List<String> dcs) throws Exception {
        final List<StorageLocation> storageLocations = new ArrayList<>();

        if (dcs == null || dcs.isEmpty()) {
            storageLocations.addAll(listNodes());
        } else {
            for (final String dc : dcs) {
                storageLocations.addAll(listNodes(dc));
            }
        }

        return storageLocations;
    }

    @Override
    public List<String> listDcs() throws Exception {
        return getDirectories(Paths.get(storageLocation.withoutNodeAndDc().replaceAll("file://", ""))).stream().map(p -> p.getFileName().toString()).collect(toList());
    }

    private List<Path> getEmptyDirectories(final StorageLocation storageLocation) {
        final List<Path> emptyDirectories = new ArrayList<>();

        final Path root = storageLocation.fileBackupDirectory
            .resolve(request.storageLocation.bucket)
            .resolve(request.storageLocation.clusterId)
            .resolve(request.storageLocation.datacenterId)
            .resolve(request.storageLocation.nodeId)
            .resolve("data");

        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
                    if (attrs.isDirectory() && isDirectoryEmpty(dir)) {
                        emptyDirectories.add(dir);
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (final Exception ex) {
            logger.debug(String.format("Unable to list directory %s. This might happen when you mount Azure File Share.", root), ex);
        }

        return emptyDirectories;
    }

    private List<Path> getDirectories(final Path directory) throws Exception {
        return Files.list(directory).filter(p -> Files.isDirectory(p) && !p.equals(directory)).collect(toList());
    }

    private boolean isDirectoryEmpty(final Path directory) throws IOException {
        try (final DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            return !stream.iterator().hasNext();
        }
    }

    @Override
    public void cleanup() {
        // Nothing to cleanup
    }
}
