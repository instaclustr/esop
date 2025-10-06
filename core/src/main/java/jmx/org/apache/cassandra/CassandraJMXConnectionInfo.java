package jmx.org.apache.cassandra;

import javax.management.remote.JMXServiceURL;
import java.net.MalformedURLException;

import com.google.common.base.MoreObjects;
import com.instaclustr.picocli.CassandraJMXSpec;

/**
 * Holder of JMX related information for setting up JMX connection to Cassandra node.
 */
public class CassandraJMXConnectionInfo {

    public final String jmxPassword;
    public final String jmxUser;
    public final String jmxCredentials;
    public final JMXServiceURL jmxServiceURL;
    public final String trustStore;
    public final String trustStorePassword;
    public final String keyStore;
    public final String keyStorePassword;
    public boolean clientAuth;

    public CassandraJMXConnectionInfo() throws MalformedURLException {
        this(null, null, null, new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:7199/jmxrmi"), null, null, null, null, false);
    }

    public CassandraJMXConnectionInfo(final CassandraJMXSpec jmxSpec) {
        this(jmxSpec.jmxPassword,
             jmxSpec.jmxUser,
             jmxSpec.jmxCredentials,
             jmxSpec.jmxServiceURL,
             jmxSpec.trustStore,
             jmxSpec.trustStorePassword,
             jmxSpec.keyStore,
             jmxSpec.keyStorePassword,
             jmxSpec.jmxClientAuth);
    }

    public CassandraJMXConnectionInfo(final String jmxPassword,
                                      final String jmxUser,
                                      final String jmxCredentials,
                                      final JMXServiceURL jmxServiceURL,
                                      final String trustStore,
                                      final String trustStorePassword,
                                      final String keyStore,
                                      final String keyStorePassword,
                                      final boolean clientAuth) {
        this.jmxPassword = jmxPassword;
        this.jmxUser = jmxUser;
        this.jmxCredentials = jmxCredentials;
        this.jmxServiceURL = jmxServiceURL;
        this.trustStore = trustStore;
        this.trustStorePassword = trustStorePassword;
        this.keyStore = keyStore;
        this.keyStorePassword = keyStorePassword;
        this.clientAuth = clientAuth;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("jmxServiceURL", jmxServiceURL)
            .add("trustStore", trustStore)
            .add("keyStore", keyStore)
            .add("jmxUser", jmxUser)
            .add("clientAuth", clientAuth)
            .add("trustStorePassword", "redacted")
            .add("keystorePassword", "redacted")
            .add("jmxCredentials", jmxCredentials)
            .add("jmxPassword", "redacted")
            .toString();
    }
}
