package com.instaclustr.esop.impl.restore;

import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.IMPORT;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.IN_PLACE;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.UNKNOWN;
import static java.lang.String.format;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.UUIDDeserializer;
import com.fasterxml.jackson.databind.ser.std.UUIDSerializer;
import com.google.common.base.MoreObjects;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.DatabaseEntities.DatabaseEntitiesConverter;
import com.instaclustr.esop.impl.DatabaseEntities.DatabaseEntitiesDeserializer;
import com.instaclustr.esop.impl.DatabaseEntities.DatabaseEntitiesSerializer;
import com.instaclustr.esop.impl.Directories;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.RenamedEntities;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl._import.ImportOperationRequest;
import com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseTypeConverter;
import com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType;
import com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyTypeConverter;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.jackson.PathSerializer;
import com.instaclustr.kubernetes.KubernetesHelper;
import com.instaclustr.picocli.typeconverter.PathTypeConverter;
import picocli.CommandLine.Option;

public class RestoreOperationRequest extends BaseRestoreOperationRequest {

    @JsonIgnore
    public final Directories dirs = new Directories(this);

    @Option(names = {"--dd", "--data-directory"},
        description = "Base directory that contains the Cassandra data, cache and commitlog directories",
        converter = PathTypeConverter.class,
        defaultValue = "/var/lib/cassandra/")
    @JsonDeserialize(using = PathDeserializer.class)
    @JsonSerialize(using = PathSerializer.class)
    public Path cassandraDirectory;

    @Option(names = {"--cd", "--config-directory"},
        description = "Directory where configuration of Cassandra is stored.",
        converter = PathTypeConverter.class,
        defaultValue = "/etc/cassandra/")
    @JsonDeserialize(using = PathDeserializer.class)
    @JsonSerialize(using = PathSerializer.class)
    public Path cassandraConfigDirectory;

    @Option(names = {"--rs", "--restore-system-keyspace"},
        description = "Restore system keyspaces too, consult option '--restore-into-new-cluster' as well.")
    public boolean restoreSystemKeyspace;

    @Option(names = {"-s", "--st", "--snapshot-tag"},
        description = "Snapshot to download and restore.",
        required = true)
    public String snapshotTag;

    @Option(names = {"--entities"},
        description = "Comma separated list of keyspaces or keyspaces and tables to restore either in form 'ks1,ks2' or 'ks1.cf1,ks2.cf2'",
        converter = DatabaseEntitiesConverter.class)
    @JsonProperty("entities")
    @JsonSerialize(using = DatabaseEntitiesSerializer.class)
    @JsonDeserialize(using = DatabaseEntitiesDeserializer.class)
    public DatabaseEntities entities;

    @Option(names = {"--update-cassandra-yaml"},
        description = "If set to true, cassandra.yaml file will be updated to restore it properly (sets initial_tokens)")
    public boolean updateCassandraYaml;

    @Option(names = {"--restoration-strategy-type"},
        description = "Strategy type to use, either IN_PLACE, IMPORT or HARDLINKS",
        converter = RestorationStrategyTypeConverter.class,
        required = true)
    public RestorationStrategyType restorationStrategyType;

    @Option(names = {"--restoration-phase-type"},
        description = "Restoration phase a particular restoration strategy is in during this request invocation, a must to specify upon IMPORT or HARDLINKS strategy",
        converter = RestorationPhaseTypeConverter.class)
    public RestorationPhaseType restorationPhase;

    @Option(names = {"--restoration-no-delete-truncates"},
        description = "Flag saying to restoration strategies which are truncating tables (import or hardlinks strategy) that it should not delete "
            + "directories where tables where truncated. After successful restoration, these data just occupy disk space so they might "
            + "be deleted on cleanup phase. This option defaults to false.")
    public boolean noDeleteTruncates;

    @Option(names = {"--restoration-no-delete-downloads"},
        description = "Flag saying for restoration strategies if it should skip cleanup of downloaded data on cleanup phase. This option defaults to false.")
    public boolean noDeleteDownloads;

    @Option(names = {"--restoration-no-download-data"},
        description = "Flag saying for restoration strategies if it should skip cleanup of downloaded data on cleanup phase. This option defaults to false.")
    public boolean noDownloadData;

    @Option(names = "--schema-version",
        description = "version of schema in case there are multiple snapshots of same name")
    public UUID schemaVersion;

    @Option(names = "--exact-schema-version",
        description = "Expect exactly same schema version of a node(s) to restore into and schema version of taken backup to restore from. Defaults to false.")
    public boolean exactSchemaVersion;

    @JsonProperty("import")
    public ImportOperationRequest importing;

    @Option(names = "--timeout",
        description = "Timeout, in hours, after which restore operation will be aborted when not finished. It defaults to 5 (hours). This "
            + "flag is effectively used only upon global requests.",
        defaultValue = "5")
    @JsonProperty("timeout")
    public int timeout;

    @JsonProperty("globalRequest")
    @Option(names = "--globalRequest",
        description = "If set, a node this tool will connect to will coordinate cluster-wide restore.")
    public boolean globalRequest;

    @JsonProperty("resolveHostIdFromTopology")
    @Option(names = "--resolve-host-id-from-topology",
        description = "If set, restoration process will translate nodeId in storage location to hostname in topology file uploaded to remote bucket upon backup "
            + "based on snapshot name and schema version.")
    public boolean resolveHostIdFromTopology;

    @Option(names = "--restore-into-new-cluster",
        description = "If set to true, IN_PLACE restoration will pick only keyspaces necessary for bootstrapping, e.g. system_schema, while all other system keyspaces will be re-generated.")
    public boolean newCluster;

    @Option(names = "--cassandra-version",
        description = "Version of Cassandra version to restore into. It is important to specify it only in case we are restoring to Cassandra 2 and "
            + "--restoration-strategy-type is IN_PLACE")
    public String cassandraVersion;

    @JsonProperty
    @Option(names = "--rename",
        description = "[OLD_KEYSPACE.OLD_TABLE]=[OLD_KEYSPACE.NEW_TABLE], if specified upon restoration, table will be restored into new table, not into the original one")
    public Map<String, String> rename = new HashMap<>();

    // this option does not make sense for Esop CLI as any request would be confined to one node only and one phase only
    // if specified in connection with Icarus, it will execute just one phase, cluster wide, and it will not advance to other phases
    // which are required in restoration, by doing this, one migh e.g.call cluster-wide download phase or cleanup phase in isolation
    @JsonProperty("singlePhase")
    public boolean singlePhase;

    @Option(names = "--datacenter",
        description = "Name of datacenter(s) against which restore will be done. "
            + "It means that nodes in a different DC will not receive restore requests. "
            + "Multiple dcs are separated by comma.")
    @JsonProperty("dc")
    public String dc;

    public RestoreOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public RestoreOperationRequest(@JsonProperty("type") final String type,
                                   @JsonProperty("storageLocation") final StorageLocation storageLocation,
                                   @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                   @JsonProperty("cassandraDirectory") final Path cassandraDirectory,
                                   @JsonProperty("cassandraConfigDirectory") final Path cassandraConfigDirectory,
                                   @JsonProperty("restoreSystemKeyspace") final boolean restoreSystemKeyspace,
                                   @JsonProperty("snapshotTag") final String snapshotTag,
                                   @JsonProperty("entities")
                                   @JsonDeserialize(using = DatabaseEntitiesDeserializer.class)
                                   @JsonSerialize(using = DatabaseEntitiesSerializer.class) final DatabaseEntities entities,
                                   @JsonProperty("updateCassandraYaml") final boolean updateCassandraYaml,
                                   @JsonProperty("restorationStrategyType") final RestorationStrategyType restorationStrategyType,
                                   @JsonProperty("restorationPhase") final RestorationPhaseType restorationPhase,
                                   @JsonProperty("import") final ImportOperationRequest importing,
                                   @JsonProperty("noDeleteTruncates") final boolean noDeleteTruncates,
                                   @JsonProperty("noDeleteDownloads") final boolean noDeleteDownloads,
                                   @JsonProperty("noDownloadData") final boolean noDownloadData,
                                   @JsonProperty("exactSchemaVersion") final boolean exactSchemaVersion,
                                   @JsonProperty("schemaVersion")
                                   @JsonDeserialize(using = UUIDDeserializer.class)
                                   @JsonSerialize(using = UUIDSerializer.class) final UUID schemaVersion,
                                   @JsonProperty("k8sNamespace") final String k8sNamespace,
                                   @JsonProperty("k8sSecretName") final String k8sSecretName,
                                   @JsonProperty("globalRequest") final boolean globalRequest,
                                   @JsonProperty("dc") final String dc,
                                   @JsonProperty("timeout") final Integer timeout,
                                   @JsonProperty("resolveHostIdFromTopology") final boolean resolveHostIdFromTopology,
                                   @JsonProperty("insecure") final boolean insecure,
                                   @JsonProperty("newCluster") final boolean newCluster,
                                   @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                                   @JsonProperty("proxySettings") final ProxySettings proxySettings,
                                   @JsonProperty("rename") final Map<String, String> rename,
                                   @JsonProperty("retry") final RetrySpec retry,
                                   @JsonProperty("singlePhase") final boolean singlePhase) {
        super(storageLocation, concurrentConnections, k8sNamespace, k8sSecretName, insecure, skipBucketVerification, proxySettings, retry);
        this.cassandraDirectory = (cassandraDirectory == null || cassandraDirectory.toFile().getAbsolutePath().equals("/")) ? Paths.get("/var/lib/cassandra") : cassandraDirectory;
        this.cassandraConfigDirectory = cassandraConfigDirectory == null ? Paths.get("/etc/cassandra") : cassandraConfigDirectory;
        this.restoreSystemKeyspace = restoreSystemKeyspace;
        this.snapshotTag = snapshotTag;
        this.entities = entities == null ? DatabaseEntities.empty() : entities;
        this.updateCassandraYaml = updateCassandraYaml;
        this.restorationStrategyType = restorationStrategyType == null ? RestorationStrategyType.IN_PLACE : restorationStrategyType;
        this.restorationPhase = restorationPhase == null ? RestorationPhaseType.UNKNOWN : restorationPhase;
        this.importing = importing;
        this.noDeleteTruncates = noDeleteTruncates;
        this.noDeleteDownloads = noDeleteDownloads;
        this.noDownloadData = noDownloadData;
        this.schemaVersion = schemaVersion;
        this.exactSchemaVersion = exactSchemaVersion;
        this.globalRequest = globalRequest;
        this.dc = dc;
        this.type = type;
        this.timeout = timeout == null || timeout < 1 ? 5 : timeout;
        this.resolveHostIdFromTopology = resolveHostIdFromTopology;
        this.newCluster = newCluster;
        this.rename = rename == null ? Collections.emptyMap() : rename;
        this.singlePhase = singlePhase;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("storageLocation", storageLocation)
            .add("concurrentConnections", concurrentConnections)
            .add("cassandraDirectory", cassandraDirectory)
            .add("restoreSystemKeyspace", restoreSystemKeyspace)
            .add("snapshotTag", snapshotTag)
            .add("entities", entities)
            .add("restorationStrategyType", restorationStrategyType)
            .add("restorationPhase", restorationPhase)
            .add("import", importing)
            .add("noDeleteTruncates", noDeleteTruncates)
            .add("noDeleteDownloads", noDeleteDownloads)
            .add("noDownloadData", noDownloadData)
            .add("schemaVersion", schemaVersion)
            .add("exactSchemaVersion", exactSchemaVersion)
            .add("updateCassandraYaml", updateCassandraYaml)
            .add("k8sNamespace", k8sNamespace)
            .add("k8sSecretName", k8sSecretName)
            .add("globalRequest", globalRequest)
            .add("dc", dc)
            .add("timeout", timeout)
            .add("resolveHostIdFromTopology", resolveHostIdFromTopology)
            .add("insecure", insecure)
            .add("newCluster", newCluster)
            .add("skipBucketVerification", skipBucketVerification)
            .add("proxySettings", proxySettings)
            .add("cassandraVersion", cassandraVersion)
            .add("rename", rename)
            .add("retry", retry)
            .add("singlePhase", singlePhase)
            .toString();
    }

    @JsonIgnore
    public void validate(Set<String> storageProviders) {
        super.validate(storageProviders);

        if (this.snapshotTag == null || this.snapshotTag.isEmpty()) {
            throw new IllegalStateException("snapshotTag can not be blank!");
        }

        if (this.restorationPhase == null) {
            this.restorationPhase = RestorationPhaseType.UNKNOWN;
        }

        if (this.restorationStrategyType == UNKNOWN) {
            throw new IllegalStateException("restorationStrategyType is not recognized");
        }

        if (this.restorationStrategyType != IN_PLACE) {
            if (this.restorationPhase == RestorationPhaseType.UNKNOWN) {
                throw new IllegalStateException("restorationPhase is not recognized, it has to be set when you use IMPORT or HARDLINKS strategy type");
            }
        }

        if (this.restorationStrategyType == IN_PLACE && this.restorationPhase != RestorationPhaseType.UNKNOWN) {
            throw new IllegalStateException(format("you can not set restorationPhase %s when your restorationStrategyType is IN_PLACE", this.restorationPhase));
        }

        if (this.restorationStrategyType == IMPORT || this.restorationStrategyType == HARDLINKS) {
            if (this.importing == null) {
                throw new IllegalStateException(format("you can not specify %s restorationStrategyType and have 'import' field empty!", this.restorationStrategyType));
            }
        }

        if (!Files.exists(this.cassandraDirectory)) {
            throw new IllegalStateException(format("cassandraDirectory %s does not exist", this.cassandraDirectory));
        }

        if ((KubernetesHelper.isRunningInKubernetes() || KubernetesHelper.isRunningAsClient())) {
            if (this.resolveKubernetesSecretName() == null) {
                throw new IllegalStateException("This code is running in Kubernetes or as a Kubernetes client but it is not possible to resolve k8s secret name for restores!");
            }

            if (this.resolveKubernetesNamespace() == null) {
                throw new IllegalStateException("This code is running in Kubernetes or as a Kubernetes client but it is not possible to resolve k8s namespace for restores!");
            }
        }

        if (this.entities == null) {
            this.entities = DatabaseEntities.empty();
        }

        try {
            DatabaseEntities.validateForRequest(this.entities);
        } catch (final Exception ex) {
            throw new IllegalStateException(ex.getMessage());
        }

        try {
            RenamedEntities.validate(this.rename);
        } catch (final Exception ex) {
            throw new IllegalStateException("Invalid 'rename' parameter: " + ex.getMessage());
        }

        if (this.rename != null && !this.rename.isEmpty() && this.restorationStrategyType == IN_PLACE) {
            throw new IllegalStateException("rename field can not be used for in-place strategy, only for import or hardlinks");
        }

        if (importing != null && importing.sourceDir != null && cassandraDirectory != null) {
            if (importing.sourceDir.equals(cassandraDirectory.resolve("data"))) {
                throw new IllegalStateException(String.format("Directory where SSTables will be downloaded (%s) is equal to Cassandra data "
                                                                  + "directory where your keyspaces are - this is not valid. Please set "
                                                                  + "sourceDir to a destination which is not equal to nor in %s",
                                                              importing.sourceDir,
                                                              cassandraDirectory.resolve("data")));
            }

            if (importing.sourceDir.startsWith(cassandraDirectory.resolve("data"))) {
                throw new IllegalStateException(String.format("Directory where SSTables will be downloaded (%s) is inside Cassandra data "
                                                                  + "directory where your keyspaces are - this is not valid. Please set "
                                                                  + "sourceDir to a destination which does not start with %s",
                                                              importing.sourceDir,
                                                              cassandraDirectory.resolve("data")));
            }

            if (importing.sourceDir.equals(cassandraDirectory)) {
                throw new IllegalStateException(String.format("Directory where SSTables will be downloaded (%s) is equal to Cassandra directory %s",
                                                              importing.sourceDir,
                                                              cassandraDirectory));
            }
        }
    }
}
