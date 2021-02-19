package com.instaclustr.esop.topology;

import com.google.common.base.MoreObjects;
import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.esop.topology.CassandraSimpleTopology.CassandraSimpleTopologyResult;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.CassandraObjectNames.V3;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.EndpointSnitchInfoMBean;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraSimpleTopology implements CassandraInteraction<CassandraSimpleTopologyResult> {

    private final CassandraJMXService jmxService;

    public CassandraSimpleTopology(final CassandraJMXService jmxService) {
        this.jmxService = jmxService;
    }

    @Override
    public CassandraSimpleTopologyResult act() throws Exception {
        final String clusterName = new CassandraClusterName(jmxService).act();

        final String dc = jmxService.doWithMBean(new FunctionWithEx<EndpointSnitchInfoMBean, String>() {
            @Override
            public String apply(final EndpointSnitchInfoMBean mbean) {
                return mbean.getDatacenter();
            }
        }, EndpointSnitchInfoMBean.class, V3.ENDPOINT_SNITCH_INFO_MBEAN_NAME);

        final String nodeId = jmxService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, String>() {
            @Override
            public String apply(final StorageServiceMBean object) throws Exception {
                return object.getLocalHostId();
            }
        });

        return new CassandraSimpleTopologyResult(clusterName, dc, nodeId);
    }

    public static class CassandraSimpleTopologyResult {

        private String clusterName;
        private String dc;
        private String hostId;

        public CassandraSimpleTopologyResult(final String clusterName, final String dc, final String hostId) {
            this.clusterName = clusterName;
            this.dc = dc;
            this.hostId = hostId;
        }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(final String clusterName) {
            this.clusterName = clusterName;
        }

        public String getDc() {
            return dc;
        }

        public void setDc(final String dc) {
            this.dc = dc;
        }

        public String getHostId() {
            return hostId;
        }

        public void setHostId(final String hostId) {
            this.hostId = hostId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("clusterName", clusterName)
                .add("dc", dc)
                .add("hostId", hostId)
                .toString();
        }
    }
}
