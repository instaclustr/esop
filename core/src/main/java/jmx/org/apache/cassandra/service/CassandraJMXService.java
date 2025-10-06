package jmx.org.apache.cassandra.service;

import static javax.management.JMX.newMBeanProxy;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Multimap;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import jmx.org.apache.cassandra.JMXUtils;
import jmx.org.apache.cassandra.service.cassandra2.Cassandra2StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra3.ColumnFamilyStoreMBean;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra30.Cassandra30ColumnFamilyStoreMBean;
import jmx.org.apache.cassandra.service.cassandra30.Cassandra30StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra4.Cassandra4ColumnFamilyStoreMBean;
import jmx.org.apache.cassandra.service.cassandra4.Cassandra4StorageServiceMBean;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface CassandraJMXService {

    static final Logger logger = LoggerFactory.getLogger(CassandraJMXService.class);

    // storage service mbean

    <T> T doWithCassandra4StorageServiceMBean(FunctionWithEx<Cassandra4StorageServiceMBean, T> func) throws Exception;

    <T> T doWithCassandra3StorageServiceMBean(FunctionWithEx<StorageServiceMBean, T> func) throws Exception;

    <T> T doWithCassandra30StorageServiceMBean(FunctionWithEx<Cassandra30StorageServiceMBean, T> func) throws Exception;

    <T> T doWithCassandra2StorageServiceMBean(FunctionWithEx<Cassandra2StorageServiceMBean, T> func) throws Exception;

    <T> T doWithStorageServiceMBean(FunctionWithEx<StorageServiceMBean, T> func) throws Exception;

    // column family store mbean

    <T> T doWithCassandra3ColumnFamilyStoreMBean(FunctionWithEx<ColumnFamilyStoreMBean, T> func, String keyspace, String columnFamily) throws Exception;

    <T> T doWithCassandra30ColumnFamilyStoreMBean(FunctionWithEx<Cassandra30ColumnFamilyStoreMBean, T> func, String keyspace, String columnFamily) throws Exception;

    <T> T doWithCassandra4ColumnFamilyStoreMBean(FunctionWithEx<Cassandra4ColumnFamilyStoreMBean, T> func, String keyspace, String columnFamily) throws Exception;

    Multimap<String, ColumnFamilyStoreMBean> getCFSMBeans() throws Exception;

    default <T, U> T doWithMBean(FunctionWithEx<U, T> func,
                                 Class<U> mbeanClass,
                                 ObjectName objectName) throws Exception {
        return doWithMBean(func, mbeanClass, objectName, getCassandraJmxConnectionInfo());
    }

    default <T, U> T doWithMBean(FunctionWithEx<U, T> func,
                                 Class<U> mbeanClass,
                                 ObjectName objectName,
                                 CassandraJMXConnectionInfo jmxConnectionInfo) throws Exception {

        try (JMXConnector jmxConnector = JMXUtils.getJmxConnector(jmxConnectionInfo)) {

            jmxConnector.connect();

            final MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();

            final U proxy = newMBeanProxy(mBeanServerConnection, objectName, mbeanClass);

            waitUntilRegistered(mBeanServerConnection, objectName);

            return func.apply(proxy);
        }
    }

    default <U> void waitUntilRegistered(final MBeanServerConnection mBeanServerConnection,
                                         final ObjectName objectName) {
        Awaitility.await()
            .pollInterval(1, TimeUnit.SECONDS)
            .timeout(1, TimeUnit.MINUTES)
            .until(() -> {
                boolean registered = mBeanServerConnection.isRegistered(objectName);

                if (!registered) {
                    logger.info(String.format("Waiting for %s to be registered.", objectName));
                }

                return registered;
            });
    }

    default <T, U> T doWithMBean(FunctionWithEx<U, T> func,
                                 Class<U> mbeanClass,
                                 String query,
                                 CassandraJMXConnectionInfo jmxConnectionInfo) throws Exception {

        try (JMXConnector jmxConnector = JMXUtils.getJmxConnector(jmxConnectionInfo)) {

            jmxConnector.connect();

            MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();

            Set<ObjectName> objectNames = mBeanServerConnection.queryNames(new ObjectName(query), null);

            if (objectNames.isEmpty()) {
                throw new IllegalStateException(String.format("Could not find ObjectName with query %s", query));
            }

            if (objectNames.size() != 1) {
                throw new IllegalStateException(String.format("There is more than one ObjectName returned by query %s. They are: %s",
                                                              query,
                                                              objectNames.stream().map(ObjectName::getCanonicalName)));
            }

            final U proxy = newMBeanProxy(mBeanServerConnection, objectNames.iterator().next(), mbeanClass);

            final ObjectName objectName = objectNames.iterator().next();

            waitUntilRegistered(mBeanServerConnection, objectName);

            return func.apply(proxy);
        }
    }

    CassandraJMXConnectionInfo getCassandraJmxConnectionInfo();
}
