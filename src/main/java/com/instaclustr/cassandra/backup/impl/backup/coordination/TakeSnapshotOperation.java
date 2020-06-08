package com.instaclustr.cassandra.backup.impl.backup.coordination;

import java.util.HashMap;

import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationRequest;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TakeSnapshotOperation extends Operation<TakeSnapshotOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(TakeSnapshotOperation.class);

    private final TakeSnapshotOperationRequest request;
    private final CassandraJMXService cassandraJMXService;

    public TakeSnapshotOperation(final CassandraJMXService cassandraJMXService,
                                 final TakeSnapshotOperationRequest request) {
        super(request);
        this.request = request;
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    protected void run0() throws Exception {
        if (request.entities.areEmpty()) {
            logger.info("Taking snapshot '{}' on all keyspaces.", request.tag);
        } else {
            logger.info("Taking snapshot '{}' on {}", request.tag, request.entities);
        }

        cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Void>() {
            @Override
            public Void apply(StorageServiceMBean ssMBean) throws Exception {

                if (request.entities.areEmpty()) {
                    ssMBean.takeSnapshot(request.tag, new HashMap<>());
                } else {
                    ssMBean.takeSnapshot(request.tag, new HashMap<>(), DatabaseEntities.forTakingSnapshot(request.entities));
                }

                return null;
            }
        });
    }

    public static class TakeSnapshotOperationRequest extends OperationRequest {

        final DatabaseEntities entities;
        final String tag;

        public TakeSnapshotOperationRequest(final DatabaseEntities entities, final String tag) {
            this.entities = entities;
            this.tag = tag;
        }
    }
}
