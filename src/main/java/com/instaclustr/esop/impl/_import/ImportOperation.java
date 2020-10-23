package com.instaclustr.esop.impl._import;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.nio.file.Files.exists;

import javax.validation.constraints.NotNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ext.NioPathDeserializer;
import com.fasterxml.jackson.databind.ext.NioPathSerializer;
import com.google.common.collect.Sets;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationFailureException;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra4.Cassandra4ColumnFamilyStoreMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportOperation extends Operation<ImportOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(ImportOperationRequest.class);

    private final CassandraJMXService cassandraJMXService;
    private final CassandraVersion cassandraVersion;

    @AssistedInject
    public ImportOperation(final CassandraJMXService cassandraJMXService,
                           final CassandraVersion cassandraVersion,
                           @Assisted final ImportOperationRequest request) {
        super(request);

        this.cassandraJMXService = cassandraJMXService;
        this.cassandraVersion = cassandraVersion;
    }

    // this constructor is not meant to be instantiated manually
    // and it fulfills the purpose of deserialisation from JSON string to an Operation object, currently just for testing purposes
    @JsonCreator
    private ImportOperation(@JsonProperty("type") final String type,
                            @JsonProperty("id") final UUID id,
                            @JsonProperty("creationTime") final Instant creationTime,
                            @JsonProperty("state") final State state,
                            @JsonProperty("errors") final List<Error> errors,
                            @JsonProperty("progress") final float progress,
                            @JsonProperty("startTime") final Instant startTime,
                            @JsonProperty("keyspace") final String keyspace,
                            @JsonProperty("table") final String table,
                            @JsonProperty("keepLevel") final boolean keepLevel,
                            @JsonProperty("noVerify") final boolean noVerify,
                            @JsonProperty("noVerifyTokens") final boolean noVerifyTokens,
                            @JsonProperty("noInvalidateCaches") final boolean noInvalidateCaches,
                            @JsonProperty("quick") final boolean quick,
                            @JsonProperty("extendedVerify") final boolean extendedVerify,
                            @NotNull
                            @JsonProperty("sourceDir")
                            @JsonDeserialize(using = NioPathDeserializer.class)
                            @JsonSerialize(using = NioPathSerializer.class) final Path sourceDir) {
        super(type,
              id,
              creationTime,
              state,
              errors,
              progress,
              startTime,
              new ImportOperationRequest(type,
                                         keyspace,
                                         table,
                                         keepLevel,
                                         noVerify,
                                         noVerifyTokens,
                                         noInvalidateCaches,
                                         quick,
                                         extendedVerify,
                                         sourceDir));
        this.cassandraJMXService = null;
        this.cassandraVersion = null;
    }

    @Override
    protected void run0() throws Exception {

        assert cassandraJMXService != null;
        assert cassandraVersion != null;

        if (isNullOrEmpty(request.keyspace)) {
            throw new IllegalStateException("keyspace was not specified!");
        }

        if (isNullOrEmpty(request.table)) {
            throw new IllegalStateException("table was not specified!");
        }

        if (request.sourceDir == null) {
            throw new IllegalStateException("Request's source dir is not specified");
        }

        if (!exists(request.sourceDir)) {
            throw new IllegalStateException(String.format("source directory %s does not exist", request.sourceDir));
        }

        if (CassandraVersion.isFour(cassandraVersion)) {
            final List<String> failedImportDirs = cassandraJMXService.doWithCassandra4ColumnFamilyStoreMBean(new FunctionWithEx<Cassandra4ColumnFamilyStoreMBean, List<String>>() {
                @Override
                public List<String> apply(final Cassandra4ColumnFamilyStoreMBean cfProxy) throws Exception {

                    logger.info(format("Importing SSTables of %s.%s", request.keyspace, request.table));

                    final Path tablePath = Files.list(request.sourceDir.resolve("data").resolve(request.keyspace))
                        .filter(table -> !table.getFileName().toString().endsWith(request.table) && table.getFileName().toString().startsWith(request.table))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException(format("There is not any directory with SSTables belonging to %s keyspace and %s table",
                                                                            request.keyspace, request.table)));

                    final List<String> failedImportDirectories = cfProxy.importNewSSTables(Sets.newHashSet(tablePath.toAbsolutePath().toString()),
                                                                                           !request.keepLevel,
                                                                                           !request.keepRepaired,
                                                                                           !request.noVerify,
                                                                                           !request.noVerifyTokens,
                                                                                           !request.noInvalidateCaches,
                                                                                           request.extendedVerify);

                    logger.info(format("Importing SSTables of %s.%s has finished.", request.keyspace, request.table));

                    return failedImportDirectories;
                }
            }, request.keyspace, request.table);

            if (failedImportDirs != null && !failedImportDirs.isEmpty()) {
                throw new OperationFailureException(format("Failed to import SSTable directories %s.", failedImportDirs.toString()));
            }
        } else if (CassandraVersion.isThree(cassandraVersion)) {
            cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Object>() {
                @Override
                public Object apply(final StorageServiceMBean storageServiceMBean) {

                    logger.info(format("Loading SSTables of %s.%s", request.keyspace, request.table));

                    storageServiceMBean.loadNewSSTables(request.keyspace, request.table);

                    logger.info(format("Loading SSTables of %s.%s has finished.", request.keyspace, request.table));

                    return null;
                }
            });
        } else {
            throw new OperationFailureException(format("Underlying version of Cassandra is not supported to import (v4) nor load (v3) SSTables: %s", cassandraVersion.toString()));
        }
    }
}
