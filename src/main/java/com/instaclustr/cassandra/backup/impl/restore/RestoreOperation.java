package com.instaclustr.cassandra.backup.impl.restore;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.UUIDDeserializer;
import com.fasterxml.jackson.databind.ser.std.UUIDSerializer;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl._import.ImportOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.OperationCoordinator.OperationCoordinatorException;
import com.instaclustr.operations.OperationFailureException;
import com.instaclustr.operations.ResultGatherer;

public class RestoreOperation extends Operation<RestoreOperationRequest> implements Cloneable {

    private final OperationCoordinator<RestoreOperationRequest> coordinator;

    @AssistedInject
    public RestoreOperation(Optional<OperationCoordinator<RestoreOperationRequest>> coordinator,
                            @Assisted final RestoreOperationRequest request) {
        super(request);

        if (!coordinator.isPresent()) {
            throw new OperationFailureException("There is no operation coordinator.");
        }

        this.coordinator = coordinator.get();
    }

    public RestoreOperation(final RestoreOperationRequest request) {
        super(request);
        this.coordinator = null;
        this.type = "restore";
    }

    @JsonCreator
    private RestoreOperation(@JsonProperty("type") final String type,
                             @JsonProperty("id") final UUID id,
                             @JsonProperty("creationTime") final Instant creationTime,
                             @JsonProperty("state") final State state,
                             @JsonProperty("failureCause") final Throwable failureCause,
                             @JsonProperty("progress") final float progress,
                             @JsonProperty("startTime") final Instant startTime,
                             @JsonProperty("storageLocation") final StorageLocation storageLocation,
                             @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                             @JsonProperty("lockFile") final Path lockFile,
                             @JsonProperty("cassandraDirectory") final Path cassandraDirectory,
                             @JsonProperty("cassandraConfigDirectory") final Path cassandraConfigDirectory,
                             @JsonProperty("restoreSystemKeyspace") final boolean restoreSystemKeyspace,
                             @JsonProperty("snapshotTag") final String snapshotTag,
                             @JsonProperty("entities") final DatabaseEntities entities,
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
                             @JsonProperty("globalRequest") final boolean globalRequest) {
        super(type, id, creationTime, state, failureCause, progress, startTime, new RestoreOperationRequest(type,
                                                                                                            storageLocation,
                                                                                                            concurrentConnections,
                                                                                                            lockFile,
                                                                                                            cassandraDirectory,
                                                                                                            cassandraConfigDirectory,
                                                                                                            restoreSystemKeyspace,
                                                                                                            snapshotTag,
                                                                                                            entities,
                                                                                                            updateCassandraYaml,
                                                                                                            restorationStrategyType,
                                                                                                            restorationPhase,
                                                                                                            importing,
                                                                                                            noDeleteTruncates,
                                                                                                            noDeleteDownloads,
                                                                                                            noDownloadData,
                                                                                                            exactSchemaVersion,
                                                                                                            schemaVersion,
                                                                                                            k8sNamespace,
                                                                                                            k8sSecretName,
                                                                                                            globalRequest));
        this.coordinator = null;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    protected void run0() throws Exception {
        assert coordinator != null;

        final ResultGatherer<RestoreOperationRequest> coordinatorResult = coordinator.coordinate(this);

        if (coordinatorResult.hasErrors()) {
            throw new OperationCoordinatorException(coordinatorResult.getErrorneousOperations().toString());
        }
    }
}
