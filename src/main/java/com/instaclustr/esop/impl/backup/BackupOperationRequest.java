package com.instaclustr.esop.impl.backup;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.DatabaseEntities.DatabaseEntitiesConverter;
import com.instaclustr.esop.impl.DatabaseEntities.DatabaseEntitiesDeserializer;
import com.instaclustr.esop.impl.DatabaseEntities.DatabaseEntitiesSerializer;
import com.instaclustr.esop.impl.ListPathSerializer;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import picocli.CommandLine.Option;
import software.amazon.awssdk.services.s3.model.MetadataDirective;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BackupOperationRequest extends BaseBackupOperationRequest {

    @Option(names = {"-s", "--st", "--snapshot-tag"},
        description = "Snapshot tag name. Default is equiv. to 'autosnap-`date +s`'")
    @JsonProperty("snapshotTag")
    public String snapshotTag = format("autosnap-%d", MILLISECONDS.toSeconds(currentTimeMillis()));

    @Option(names = "--entities",
        description = "entities to backup, if not specified, all keyspaces will be backed up, form 'ks1,ks2,ks2' or 'ks1.cf1,ks2.cf2'",
        converter = DatabaseEntitiesConverter.class)
    @JsonProperty("entities")
    @JsonSerialize(using = DatabaseEntitiesSerializer.class)
    @JsonDeserialize(using = DatabaseEntitiesDeserializer.class)
    public DatabaseEntities entities;

    @Option(names = "--datacenter",
        description = "Name of datacenter against which backup will be done. It means that nodes in a different DC will not receive backup requests. "
            + "Multiple dcs are separated by comma. This is valid only in case globalRequest is true.")
    @JsonProperty("dc")
    public String dc;

    @Option(names = "--timeout",
        description = "Timeout, in hours, after which backup operation will be aborted when not finished. It defaults to 5 (hours). This "
            + "flag is effectively used only upon global requests.",
        defaultValue = "5")
    @JsonProperty("timeout")
    public int timeout;

    @JsonProperty("globalRequest")
    @Option(names = "--globalRequest",
        description = "If set, a node this tool will connect to will coordinate cluster-wide backup.")
    public boolean globalRequest;

    // this field is not "settable", it will be rewritten by backup logic if it is set
    @JsonProperty("schemaVersion")
    public String schemaVersion;

    @JsonProperty("uploadClusterTopology")
    @Option(names = "--uploadClusterTopology",
        description = "If set, a cluster topology file will be uploaded alongside a backup, defaults to false. This flag is "
            + "implicitly set to true if a request is global - coordinator node will upload this file every time.")
    public boolean uploadClusterTopology;

    public BackupOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public BackupOperationRequest(@JsonProperty("type") final String type,
                                  @JsonProperty("storageLocation") final StorageLocation storageLocation,
                                  @JsonProperty("duration") final Time duration,
                                  @JsonProperty("bandwidth") final DataRate bandwidth,
                                  @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                  @JsonProperty("metadataDirective") final MetadataDirective metadataDirective,
                                  @JsonProperty("entities")
                                  @JsonSerialize(using = DatabaseEntitiesSerializer.class)
                                  @JsonDeserialize(using = DatabaseEntitiesDeserializer.class) final DatabaseEntities entities,
                                  @JsonProperty("snapshotTag") final String snapshotTag,
                                  @JsonProperty("globalRequest") final boolean globalRequest,
                                  @JsonProperty("dc") final String dc,
                                  @JsonProperty("timeout") final Integer timeout,
                                  @JsonProperty("insecure") final boolean insecure,
                                  @JsonProperty("createMissingBucket") final boolean createMissingBucket,
                                  @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                                  @JsonProperty("schemaVersion") final String schemaVersion,
                                  @JsonProperty("uploadClusterTopology") final boolean uploadClusterTopology,
                                  @JsonProperty("proxySettings") final ProxySettings proxySettings,
                                  @JsonProperty("retry") final RetrySpec retry,
                                  @JsonProperty("skipRefreshing") final boolean skipRefreshing,
                                  @JsonSerialize(using = ListPathSerializer.class)
                                  @JsonDeserialize(contentUsing = PathDeserializer.class)
                                  @JsonProperty("dataDirs") final List<Path> dataDirs,
                                  @JsonProperty("kmsKeyId") final String kmsKeyId,
                                  @JsonProperty("gcpUniformBucketLevelAccess") final boolean gcpUniformBucketLevelAccess) {
        super(storageLocation,
              duration,
              bandwidth,
              concurrentConnections,
              metadataDirective,
              insecure,
              createMissingBucket,
              skipBucketVerification,
              proxySettings,
              retry,
              skipRefreshing,
              dataDirs,
              kmsKeyId,
              gcpUniformBucketLevelAccess);
        this.entities = entities == null ? DatabaseEntities.empty() : entities;
        this.snapshotTag = snapshotTag == null ? format("autosnap-%d", MILLISECONDS.toSeconds(currentTimeMillis())) : snapshotTag;
        this.globalRequest = globalRequest;
        this.type = type;
        this.dc = dc;
        this.timeout = timeout == null || timeout < 1 ? 5 : timeout;
        this.schemaVersion = schemaVersion;
        this.uploadClusterTopology = uploadClusterTopology;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("storageLocation", storageLocation)
            .add("duration", duration)
            .add("bandwidth", bandwidth)
            .add("concurrentConnections", concurrentConnections)
            .add("dataDirs", dataDirs)
            .add("entities", entities)
            .add("snapshotTag", snapshotTag)
            .add("globalRequest", globalRequest)
            .add("dc", dc)
            .add("timeout", timeout)
            .add("metadataDirective", metadataDirective)
            .add("insecure", insecure)
            .add("schemaVersion", schemaVersion)
            .add("uploadClusterTopology", uploadClusterTopology)
            .add("createMissingBucket", createMissingBucket)
            .add("skipBucketVerification", skipBucketVerification)
            .add("proxySettings", proxySettings)
            .add("retry", retry)
            .add("skipRefreshing", skipRefreshing)
            .add("kmsKeyId", kmsKeyId)
            .add("gcpUniformBucketLevelAccess", gcpUniformBucketLevelAccess)
            .toString();
    }

    @JsonIgnore
    public void validate(final Set<String> storageProviders) {
        super.validate(storageProviders);

        if (this.entities == null) {
            this.entities = DatabaseEntities.empty();
        }

        if (this.dataDirs == null || this.dataDirs.isEmpty()) {
            throw new IllegalStateException("You have not specified any --data-dir on command line " +
                                            "or respective field in JSON request.");
        }

        try {
            DatabaseEntities.validateForRequest(this.entities);
        } catch (final Exception ex) {
            throw new IllegalStateException(ex.getMessage());
        }
    }
}
