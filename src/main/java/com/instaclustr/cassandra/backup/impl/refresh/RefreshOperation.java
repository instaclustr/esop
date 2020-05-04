package com.instaclustr.cassandra.backup.impl.refresh;

import static java.lang.String.format;

import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RefreshOperation extends Operation<RefreshOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RefreshOperation.class);

    private final CassandraJMXService cassandraJMXService;

    public RefreshOperation(final CassandraJMXService cassandraJMXService, final RefreshOperationRequest request) {
        super(request);

        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    protected void run0() throws Exception {
        assert cassandraJMXService != null;

        cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Object>() {
            @Override
            public Object apply(final StorageServiceMBean object) throws Exception {

                object.loadNewSSTables(request.keyspace, request.table);

                logger.info(format("Refreshed table %s in keyspace %s", request.table, request.keyspace));

                return null;
            }
        });
    }
}
