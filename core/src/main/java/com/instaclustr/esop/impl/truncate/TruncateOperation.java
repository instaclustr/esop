package com.instaclustr.esop.impl.truncate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

import static java.lang.String.format;

public class TruncateOperation extends Operation<TruncateOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(TruncateOperation.class);

    private final CassandraJMXService cassandraJMXService;

    @AssistedInject
    public TruncateOperation(final CassandraJMXService cassandraJMXService,
                             @Assisted final TruncateOperationRequest request) {
        super(request);

        this.cassandraJMXService = cassandraJMXService;
    }

    // this constructor is not meant to be instantiated manually
    // and it fulfills the purpose of deserialisation from JSON string to an Operation object, currently just for testing purposes
    @JsonCreator
    private TruncateOperation(@JsonProperty("type") final String type,
                              @JsonProperty("id") final UUID id,
                              @JsonProperty("creationTime") final Instant creationTime,
                              @JsonProperty("state") final State state,
                              @JsonProperty("errors") final List<Error> errors,
                              @JsonProperty("progress") final float progress,
                              @JsonProperty("startTime") final Instant startTime,
                              @JsonProperty("keyspace") final String keyspace,
                              @JsonProperty("table") final String table) {
        super(type, id, creationTime, state, errors, progress, startTime, new TruncateOperationRequest(type, keyspace, table));
        this.cassandraJMXService = null;
    }

    @Override
    protected void run0() throws Exception {

        assert cassandraJMXService != null;

        cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Object>() {
            @Override
            public Object apply(final StorageServiceMBean serviceMBean) throws Exception {

                logger.info(format("Truncating %s.%s", request.keyspace, request.table));

                serviceMBean.truncate(request.keyspace, request.table);

                logger.info(format("Truncating %s.%s has finished.", request.keyspace, request.table));

                return null;
            }
        });
    }
}
