package com.instaclustr.esop.impl.restore.strategy;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.restore.DownloadTracker;
import com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class RestorationContext {

    public CassandraJMXService jmx;
    public ObjectMapper objectMapper;
    public Operation<RestoreOperationRequest> operation;
    public Restorer restorer;
    public CassandraVersion cassandraVersion;
    public String schemaVersion;
    public RestorationPhaseType phaseType;
    public DownloadTracker downloadTracker;
    public Map<String, BucketServiceFactory> bucketServiceFactoryMap;
}
