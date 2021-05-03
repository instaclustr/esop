package com.instaclustr.esop.impl;

import static java.lang.String.format;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.MoreObjects;
import picocli.CommandLine;
import picocli.CommandLine.ITypeConverter;

public class StorageLocation {

    private static final Pattern filePattern = Pattern.compile("(.*)://(.*)/(.*)/(.*)/(.*)/(.*)");
    private static final Pattern cloudPattern = Pattern.compile("(.*)://(.*)/(.*)/(.*)/(.*)");
    private static final Pattern globalPattern = Pattern.compile("(.*)://(.*)");

    public String rawLocation;
    public String storageProvider;
    public String bucket;
    public String clusterId;
    public String datacenterId;
    public String nodeId;
    public Path fileBackupDirectory;
    public boolean cloudLocation;
    public boolean globalRequest;

    public StorageLocation(final String rawLocation) {

        if (rawLocation.endsWith("/")) {
            this.rawLocation = rawLocation.substring(0, rawLocation.length() - 1);
        } else {
            this.rawLocation = rawLocation;
        }

        if (this.rawLocation.startsWith("file")) {
            initializeFileBackupLocation(this.rawLocation);
        } else {
            cloudLocation = true;
            initializeCloudLocation(this.rawLocation);
        }
    }

    private void initializeFileBackupLocation(final String backupLocation) {
        final Matcher matcher = filePattern.matcher(backupLocation);

        if (!matcher.matches()) {
            return;
        }

        this.rawLocation = matcher.group();
        this.storageProvider = matcher.group(1);
        this.fileBackupDirectory = Paths.get(matcher.group(2));
        this.bucket = matcher.group(3);
        this.clusterId = matcher.group(4);
        this.datacenterId = matcher.group(5);
        this.nodeId = matcher.group(6);

        if (fileBackupDirectory.toString().isEmpty()) {
            fileBackupDirectory = fileBackupDirectory.toAbsolutePath();
        }
    }

    private void initializeCloudLocation(final String storageLocation) {

        final Matcher globalMatcher = globalPattern.matcher(storageLocation);

        if (globalMatcher.matches() && globalMatcher.groupCount() == 2 && !globalMatcher.group(2).contains("/")) {
            this.rawLocation = globalMatcher.group();
            this.storageProvider = globalMatcher.group(1);
            this.bucket = globalMatcher.group(2);
            globalRequest = true;

            return;
        }

        final Matcher matcher = cloudPattern.matcher(storageLocation);

        if (matcher.matches()) {
            this.rawLocation = matcher.group();
            this.storageProvider = matcher.group(1);
            this.bucket = matcher.group(2);
            this.clusterId = matcher.group(3);
            this.datacenterId = matcher.group(4);
            this.nodeId = matcher.group(5);
        }
    }

    public void validate() throws IllegalStateException {
        if (cloudLocation) {
            if (!globalRequest) {
                if (rawLocation == null || storageProvider == null || bucket == null || clusterId == null || datacenterId == null || nodeId == null) {
                    throw new IllegalStateException(format("Storage location %s is not in form protocol://bucketName/clusterId/datacenterid/nodeId",
                                                           rawLocation));
                }
            } else if (rawLocation == null || storageProvider == null || bucket == null) {
                throw new IllegalStateException(format("Global storage location %s is not in form protocol://bucketName", rawLocation));
            }
        } else if (rawLocation == null || storageProvider == null || bucket == null || clusterId == null || datacenterId == null || nodeId == null || fileBackupDirectory == null) {
            throw new IllegalStateException(format("Storage location %s is not in form file:///some/backup/path/clusterId/datacenterId/nodeId",
                                                   rawLocation));
        }

        if (bucket.endsWith("/")) {
            throw new IllegalStateException(format("Wrong bucket name: %s", bucket));
        }

        if (clusterId != null && clusterId.endsWith("/")) {
            throw new IllegalStateException(format("Wrong cluster name: %s", clusterId));
        }

        if (datacenterId != null && datacenterId.endsWith("/")) {
            throw new IllegalStateException(format("Wrong datacenter name: %s", datacenterId));
        }

        if (nodeId != null && nodeId.endsWith("/")) {
            throw new IllegalStateException(format("Wrong node name: %s", nodeId));
        }
    }

    public String nodePath() {
        return String.format("%s/%s/%s", clusterId, datacenterId, nodeId);
    }

    public static StorageLocation updateClusterName(final StorageLocation oldLocation, final String clusterName) {
        final String withoutNodeId = oldLocation.rawLocation.substring(0, oldLocation.rawLocation.lastIndexOf("/"));
        final String withoutDatacenter = withoutNodeId.substring(0, withoutNodeId.lastIndexOf("/"));
        final String withoutClusterName = withoutDatacenter.substring(0, withoutDatacenter.lastIndexOf("/"));
        return new StorageLocation(withoutClusterName + "/" + clusterName + "/" + oldLocation.datacenterId + "/" + oldLocation.nodeId);
    }

    public static StorageLocation updateDatacenter(final StorageLocation oldLocation, final String dc) {
        final String withoutNodeId = oldLocation.rawLocation.substring(0, oldLocation.rawLocation.lastIndexOf("/"));
        final String withoutDatacenter = withoutNodeId.substring(0, withoutNodeId.lastIndexOf("/"));
        return new StorageLocation(withoutDatacenter + "/" + dc + "/" + oldLocation.nodeId);
    }

    public static StorageLocation updateNodeId(final StorageLocation oldLocation, String nodeId) {
        return new StorageLocation(oldLocation.rawLocation.substring(0, oldLocation.rawLocation.lastIndexOf("/") + 1) + nodeId);
    }

    public static StorageLocation updateNodeId(final StorageLocation oldLocation, UUID nodeId) {
        return StorageLocation.updateNodeId(oldLocation, nodeId.toString());
    }

    public static StorageLocation update(final StorageLocation oldLocation, final String clusterName, final String datacenterId, final String hostId) {
        if (oldLocation.globalRequest) {
            return new StorageLocation(String.format("%s/%s/%s/%s", oldLocation.rawLocation, clusterName, datacenterId, hostId));
        } else {
            final StorageLocation updatedNodeId = updateNodeId(oldLocation, hostId);
            final StorageLocation updatedDatacenter = updateDatacenter(updatedNodeId, datacenterId);
            return updateClusterName(updatedDatacenter, clusterName);
        }
    }

    public String withoutNode() {
        return rawLocation.substring(0, rawLocation.lastIndexOf("/"));
    }

    public String withoutNodeAndDc() {
        final String withoutNode = withoutNode();
        return withoutNode.substring(0, withoutNode.lastIndexOf("/"));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("rawLocation", rawLocation)
            .add("storageProvider", storageProvider)
            .add("bucket", bucket)
            .add("clusterId", clusterId)
            .add("datacenterId", datacenterId)
            .add("nodeId", nodeId)
            .add("fileBackupDirectory", fileBackupDirectory)
            .add("cloudLocation", cloudLocation)
            .toString();
    }

    public static class StorageLocationTypeConverter implements ITypeConverter<StorageLocation> {

        @Override
        public StorageLocation convert(final String value) throws Exception {
            if (value == null) {
                return null;
            }

            try {
                return new StorageLocation(value);
            } catch (final Exception ex) {
                throw new CommandLine.TypeConversionException(format("Invalid value of StorageLocation '%s', reason: %s",
                                                                     value,
                                                                     ex.getLocalizedMessage()));
            }
        }
    }

    public static class StorageLocationSerializer extends StdSerializer<StorageLocation> {

        public StorageLocationSerializer() {
            super(StorageLocation.class);
        }

        protected StorageLocationSerializer(final Class<StorageLocation> t) {
            super(t);
        }

        @Override
        public void serialize(final StorageLocation value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
            if (value != null) {
                gen.writeString(value.rawLocation);
            }
        }
    }

    public static class StorageLocationDeserializer extends StdDeserializer<StorageLocation> {

        public StorageLocationDeserializer() {
            super(StorageLocation.class);
        }

        @Override
        public StorageLocation deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
            final String valueAsString = p.getValueAsString();

            if (valueAsString == null) {
                return null;
            }

            try {
                return new StorageLocation(valueAsString);
            } catch (final Exception ex) {
                throw new InvalidFormatException(p, "Invalid StorageLocation.", valueAsString, StorageLocation.class);
            }
        }
    }
}
