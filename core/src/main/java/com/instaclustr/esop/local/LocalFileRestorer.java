package com.instaclustr.esop.local;

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
import java.util.Collections;
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

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
        this.localFileRestorer = this;
    }

    @AssistedInject
    public LocalFileRestorer(@Assisted final RemoveBackupRequest request,
                             ObjectMapper objectMapper) {
        super(request);
        this.objectMapper = objectMapper;
        this.localFileRestorer = this;
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
    public void downloadFile(final Path localFilePath, ManifestEntry manifestEntry, final RemoteObjectReference objectReference) throws Exception {
        final Path remoteFilePath = request.storageLocation.fileBackupDirectory
            .resolve(request.storageLocation.bucket)
            .resolve(Paths.get(objectReference.canonicalPath));

        //Assume that any path passed in to this function is a file
        Files.createDirectories(localFilePath.getParent());

        Files.copy(remoteFilePath, localFilePath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public String downloadTopology(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {

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
    public String downloadManifest(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final Path pathToList = Paths.get(request.storageLocation.rawLocation.replaceAll("file://", "")).resolve(remotePrefix);
        final String blobItem = getManifest(pathToList, keyFilter, remotePrefix);
        return new String(Files.readAllBytes(Paths.get(blobItem)));
    }

    @Override
    public String downloadNodeFile(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final Path pathToList = Paths.get(request.storageLocation.rawLocation.replaceAll("file://", "")).resolve(remotePrefix);
        final String blobItem = getFileToDownload(pathToList, keyFilter, remotePrefix);
        return new String(Files.readAllBytes(Paths.get(blobItem)));
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
        Path path;
        if (!storageLocation.cloudLocation)
             path = Paths.get(storageLocation.rawLocation.replaceAll("file://", ""), "manifests");
        else
            path = Paths.get(localFileRestorer.storageLocation.rawLocation.replaceAll("file://", ""), "manifests");

        if (!Files.exists(path))
            return Collections.emptyList();

        final List<Path> manifests = Files.list(path)
            .sorted(new ManifestAgePathComparator())
            .collect(toList());

        final List<Manifest> manifestsList = new ArrayList<>();

        for (final Path manifest : manifests) {
            final Manifest read = Manifest.read(manifest, objectMapper);
            read.setManifest(new ManifestEntry(Paths.get("manifests", manifest.getFileName().toString()), manifest, Type.FILE, null, null));
            manifestsList.add(read);
        }

        return manifestsList;
    }

    @Override
    public void delete(Path objectKey, boolean nodeAware) throws Exception {
        Path fileToDelete;
        if (nodeAware) {
            RemoteObjectReference remoteObjectReference = objectKeyToNodeAwareRemoteReference(objectKey);
            fileToDelete = request.storageLocation.fileBackupDirectory
                    .resolve(request.storageLocation.bucket)
                    .resolve(remoteObjectReference.canonicalPath);
        } else {
            fileToDelete = request.storageLocation.fileBackupDirectory
                    .resolve(request.storageLocation.bucket)
                    .resolve(objectKey);
        }

        Files.deleteIfExists(fileToDelete);
    }

    @Override
    public void delete(final ManifestReport backupToDelete, final RemoveBackupRequest request) throws Exception {
        logger.info("Deleting backup {}", backupToDelete.name);
        if (backupToDelete.reclaimableSpace > 0 && !backupToDelete.getRemovableEntries().isEmpty()) {
            for (final String removableEntry : backupToDelete.getRemovableEntries()) {
                if (!request.dry) {
                    deleteNodeAwareKey(Paths.get(removableEntry));
                } else {
                    logger.info("Deletion of {} was executed in dry mode.", removableEntry);
                }
            }
        }

        // manifest and topology as the last
        if (!request.dry) {
            deleteNodeAwareKey(backupToDelete.manifest.objectKey);
        } else {
            logger.info("Deletion of manifest {} was executed in dry mode.", backupToDelete.manifest.objectKey);
        }

        removeEmptyDirectories(storageLocation
                                       .fileBackupDirectory
                                       .resolve(request.storageLocation.bucket), request.dry);
    }

    @Override
    public void deleteTopology(String name) throws Exception {
        super.deleteTopology(name);
        try {
            Files.delete(storageLocation.fileBackupDirectory.resolve(storageLocation.bucket).resolve("topology"));
        } catch (final Exception ex) {

        }
    }

    private void removeEmptyDirectories(Path root, boolean dry) throws Exception {
        if (!dry && request.storageLocation.storageProvider.equals("file")) {
            List<Path> emptyDirectories = getEmptyDirectories(root);
            while (!emptyDirectories.isEmpty()) {
                for (final Path emptyDir : emptyDirectories) {
                    if (emptyDir.equals(root))
                        break;

                    if (!dry) {
                        logger.debug("Removing empty directory {}", emptyDir);
                        Files.delete(emptyDir);
                    }
                }

                emptyDirectories = getEmptyDirectories(root);
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
        return getDirectories(Paths.get(StorageLocation
                                                .updateDatacenter(storageLocation, dc)
                                                .withoutNode()
                                                .replace("file://", "")))
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

    private List<Path> getEmptyDirectories(final Path root) {
        final List<Path> emptyDirectories = new ArrayList<>();
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
                    if (attrs.isDirectory() && isDirectoryEmpty(dir)) {
                        emptyDirectories.add(dir);
                        return FileVisitResult.CONTINUE;
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
