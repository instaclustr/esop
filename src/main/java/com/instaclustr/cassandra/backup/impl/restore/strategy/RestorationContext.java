package com.instaclustr.cassandra.backup.impl.restore.strategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.cassandra.backup.impl.restore.DownloadTracker;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
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
}
