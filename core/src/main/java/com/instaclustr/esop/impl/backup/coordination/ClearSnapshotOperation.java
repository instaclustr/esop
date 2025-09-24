package com.instaclustr.esop.impl.backup.coordination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.backup.coordination.ClearSnapshotOperation.ClearSnapshotOperationRequest;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationRequest;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class ClearSnapshotOperation extends Operation<ClearSnapshotOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(ClearSnapshotOperation.class);

    private final CassandraJMXService cassandraJMXService;
    private boolean hasRun = false;

    public ClearSnapshotOperation(final CassandraJMXService cassandraJMXService,
                                  final ClearSnapshotOperationRequest request) {
        super(request);
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    protected void run0() {
        if (hasRun) {
            return;
        }

        hasRun = true;

        try {
            cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Void>() {
                @Override
                public Void apply(StorageServiceMBean ssMBean) throws Exception {
                    if (ssMBean.getSnapshotDetails().get(request.snapshotTag) == null) {
                        logger.debug("Not cleaning snapshot {} because it does not exist", request.snapshotTag);
                        return null;
                    }
                    ssMBean.clearSnapshot(request.snapshotTag);
                    return null;
                }
            });

            logger.info("Cleared snapshot {}.", request.snapshotTag);
        } catch (final Exception ex) {
            logger.error("Failed to cleanup snapshot {}.", request.snapshotTag, ex);
            throw new RuntimeException(String.format("Failed to cleanup snapshot %s", request.snapshotTag), ex);
        }
    }

    public static class ClearSnapshotOperationRequest extends OperationRequest {

        final String snapshotTag;

        public ClearSnapshotOperationRequest(final String snapshotTag) {
            this.snapshotTag = snapshotTag;
        }
    }
}
