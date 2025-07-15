package com.instaclustr.esop.impl.backup;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.guice.StorageProviders;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.DatabaseEntities.DatabaseEntitiesDeserializer;
import com.instaclustr.esop.impl.DatabaseEntities.DatabaseEntitiesSerializer;
import com.instaclustr.esop.impl.ListPathSerializer;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.OperationFailureException;
import software.amazon.awssdk.services.s3.model.MetadataDirective;

public class BackupOperation extends Operation<BackupOperationRequest> implements Cloneable {

    private final Set<String> storageProviders;
    private final OperationCoordinator<BackupOperationRequest> coordinator;

    @AssistedInject
    public BackupOperation(Optional<OperationCoordinator<BackupOperationRequest>> coordinator,
                           @StorageProviders Set<String> storageProviders,
                           @Assisted final BackupOperationRequest request) {
        super(request);

        if (!coordinator.isPresent()) {
            throw new OperationFailureException("There is no operation coordinator.");
        }

        this.coordinator = coordinator.get();
        this.storageProviders = storageProviders;
    }

    public BackupOperation(final BackupOperationRequest request) {
        super(request);
        this.coordinator = null;
        this.storageProviders = null;
        this.type = "backup";
    }

    private AtomicBoolean closeOperation = new AtomicBoolean(false);

    // this constructor is not meant to be instantiated manually
    // and it fulfills the purpose of deserialization from JSON string to an Operation object, currently just for testing purposes
    @JsonCreator
    private BackupOperation(@JsonProperty("type") final String type,
                            @JsonProperty("id") final UUID id,
                            @JsonProperty("creationTime") final Instant creationTime,
                            @JsonProperty("state") final State state,
                            @JsonProperty("errors") final List<Error> errors,
                            @JsonProperty("progress") final float progress,
                            @JsonProperty("startTime") final Instant startTime,
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
        super(type, id, creationTime, state, errors, progress, startTime, new BackupOperationRequest(type,
                                                                                                     storageLocation,
                                                                                                     duration,
                                                                                                     bandwidth,
                                                                                                     concurrentConnections,
                                                                                                     metadataDirective,
                                                                                                     entities,
                                                                                                     snapshotTag,
                                                                                                     globalRequest,
                                                                                                     dc,
                                                                                                     timeout,
                                                                                                     insecure,
                                                                                                     createMissingBucket,
                                                                                                     skipBucketVerification,
                                                                                                     schemaVersion,
                                                                                                     uploadClusterTopology,
                                                                                                     proxySettings,
                                                                                                     retry,
                                                                                                     skipRefreshing,
                                                                                                     dataDirs,
                                                                                                     kmsKeyId,
                                                                                                     gcpUniformBucketLevelAccess));
        coordinator = null;
        storageProviders = null;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    protected void run0() throws Exception {
        assert coordinator != null;
        assert storageProviders != null;
        request.validate(storageProviders);
        coordinator.coordinate(this);
    }
}
