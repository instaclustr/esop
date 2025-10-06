package jmx.org.apache.cassandra.service;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import jmx.org.apache.cassandra.CassandraObjectNames.V2;
import jmx.org.apache.cassandra.CassandraObjectNames.V3;
import jmx.org.apache.cassandra.CassandraObjectNames.V4;
import jmx.org.apache.cassandra.JMXUtils;
import jmx.org.apache.cassandra.service.cassandra2.Cassandra2StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra3.ColumnFamilyStoreMBean;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra30.Cassandra30ColumnFamilyStoreMBean;
import jmx.org.apache.cassandra.service.cassandra30.Cassandra30StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra4.Cassandra4ColumnFamilyStoreMBean;
import jmx.org.apache.cassandra.service.cassandra4.Cassandra4StorageServiceMBean;

public class CassandraJMXServiceImpl implements CassandraJMXService {

    private final CassandraJMXConnectionInfo jmxConnectionInfo;

    public CassandraJMXServiceImpl(CassandraJMXConnectionInfo jmxConnectionInfo) {
        this.jmxConnectionInfo = jmxConnectionInfo;
    }

    @Override
    public <T> T doWithCassandra4StorageServiceMBean(final FunctionWithEx<Cassandra4StorageServiceMBean, T> func) throws Exception {
        return doWithMBean(func,
                           Cassandra4StorageServiceMBean.class,
                           V4.STORAGE_SERVICE_MBEAN_NAME,
                           jmxConnectionInfo);
    }

    @Override
    public <T> T doWithCassandra3StorageServiceMBean(final FunctionWithEx<StorageServiceMBean, T> func) throws Exception {
        return doWithStorageServiceMBean(func);
    }

    @Override
    public <T> T doWithCassandra30StorageServiceMBean(FunctionWithEx<Cassandra30StorageServiceMBean, T> func) throws Exception {
        return doWithMBean(func,
                           Cassandra30StorageServiceMBean.class,
                           V3.STORAGE_SERVICE_MBEAN_NAME,
                           jmxConnectionInfo);
    }

    @Override
    public <T> T doWithCassandra2StorageServiceMBean(final FunctionWithEx<Cassandra2StorageServiceMBean, T> func) throws Exception {
        return doWithMBean(func,
                           Cassandra2StorageServiceMBean.class,
                           V2.STORAGE_SERVICE_MBEAN_NAME,
                           jmxConnectionInfo);
    }

    @Override
    public <T> T doWithStorageServiceMBean(final FunctionWithEx<StorageServiceMBean, T> func) throws Exception {
        return doWithMBean(func,
                           StorageServiceMBean.class,
                           V3.STORAGE_SERVICE_MBEAN_NAME,
                           jmxConnectionInfo);
    }

    @Override
    public <T> T doWithCassandra3ColumnFamilyStoreMBean(final FunctionWithEx<ColumnFamilyStoreMBean, T> func,
                                                        final String keyspace,
                                                        final String columnFamily) throws Exception {
        return doWithMBean(func, ColumnFamilyStoreMBean.class,
                           getColumnFamilyMBeanObjectNameQuery(keyspace, columnFamily),
                           jmxConnectionInfo);
    }

    @Override
    public <T> T doWithCassandra30ColumnFamilyStoreMBean(FunctionWithEx<Cassandra30ColumnFamilyStoreMBean, T> func, String keyspace, String columnFamily) throws Exception {
        return doWithMBean(func,
                           Cassandra30ColumnFamilyStoreMBean.class,
                           getColumnFamilyMBeanObjectNameQuery(keyspace, columnFamily),
                           jmxConnectionInfo);
    }

    @Override
    public <T> T doWithCassandra4ColumnFamilyStoreMBean(final FunctionWithEx<Cassandra4ColumnFamilyStoreMBean, T> func,
                                                        final String keyspace,
                                                        final String columnFamily) throws Exception {
        return doWithMBean(func,
                           Cassandra4ColumnFamilyStoreMBean.class,
                           getColumnFamilyMBeanObjectNameQuery(keyspace, columnFamily),
                           jmxConnectionInfo);
    }

    @Override
    public Multimap<String, ColumnFamilyStoreMBean> getCFSMBeans() throws Exception {

        Multimap<String, ColumnFamilyStoreMBean> cfsMBeans = HashMultimap.create();

        try (JMXConnector jmxConnector = JMXUtils.getJmxConnector(jmxConnectionInfo)) {

            jmxConnector.connect();

            final MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();

            final ObjectName query = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,*");
            final Set<ObjectName> cfObjects = mBeanServerConnection.queryNames(query, null);

            for (final ObjectName name : cfObjects) {
                String keyspace = name.getKeyProperty("keyspace");
                ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(mBeanServerConnection, name, ColumnFamilyStoreMBean.class);
                cfsMBeans.put(keyspace, cfsProxy);
            }

            return cfsMBeans;
        }
    }

    @Override
    public CassandraJMXConnectionInfo getCassandraJmxConnectionInfo() {
        return jmxConnectionInfo;
    }

    private String getColumnFamilyMBeanObjectNameQuery(final String keyspace, final String columnFamily) {
        final String type = columnFamily.contains(".") ? "IndexColumnFamilies" : "ColumnFamilies";
        return "org.apache.cassandra.db:type=*" + type + ",keyspace=" + keyspace + ",columnfamily=" + columnFamily;
    }
}
