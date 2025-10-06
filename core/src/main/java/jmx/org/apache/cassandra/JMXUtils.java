package jmx.org.apache.cassandra;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JMXUtils {

    public static synchronized JMXConnector getJmxConnector(final CassandraJMXConnectionInfo jmxConnectionInfo) throws Exception {
        if (jmxConnectionInfo == null || jmxConnectionInfo.jmxServiceURL == null) {
            throw new IllegalArgumentException("passed JMXConnectionInfo is either null or its jmxServiceURL is null.");
        }

        final PropertiesFile propertiesFile = new PropertiesFile();

        if (jmxConnectionInfo.jmxCredentials != null) {
            final Properties credentialsProperties = new Properties();
            try (final FileReader reader = new FileReader(jmxConnectionInfo.jmxCredentials)) {
                credentialsProperties.load(reader);
            }

            propertiesFile.keystorePassword = credentialsProperties.getProperty("keystorePassword");
            propertiesFile.truststorePassword = credentialsProperties.getProperty("truststorePassword");
            propertiesFile.username = credentialsProperties.getProperty("username");
            propertiesFile.password = credentialsProperties.getProperty("password");
        }

        final Map<String, Object> envMap = new HashMap<>();

        String username = jmxConnectionInfo.jmxUser;
        String password = jmxConnectionInfo.jmxPassword;

        if (propertiesFile.username != null)
            username = propertiesFile.username;

        if (propertiesFile.password != null)
            password = propertiesFile.password;

        if (username != null && password != null) {
            envMap.put(JMXConnector.CREDENTIALS, new String[]{username, password});
        }

        String keystore = jmxConnectionInfo.keyStore;
        String keystorePassword = jmxConnectionInfo.keyStorePassword;
        String trustStore = jmxConnectionInfo.trustStore;
        String trustStorePassword = jmxConnectionInfo.trustStorePassword;

        if (propertiesFile.keystorePassword != null)
            keystorePassword = propertiesFile.keystorePassword;

        if (propertiesFile.truststorePassword != null)
            trustStorePassword = propertiesFile.truststorePassword;

        if (trustStore != null && trustStorePassword != null) {
            if (!Paths.get(trustStore).toFile().exists()) {
                throw new IllegalStateException(String.format("Specified truststore file for Cassandra %s does not exist!", jmxConnectionInfo.trustStore));
            }

            System.setProperty("javax.net.ssl.trustStore", trustStore);
            System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
            System.setProperty("javax.net.ssl.keyStore", keystore);
            System.setProperty("javax.net.ssl.keyStorePassword", keystorePassword);
            System.setProperty("ssl.enable", "true");
            System.setProperty("com.sun.management.jmxremote.ssl.need.client.auth", Boolean.toString(jmxConnectionInfo.clientAuth));
            System.setProperty("com.sun.management.jmxremote.registry.ssl", "true");

            envMap.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
        }

        return envMap.isEmpty() ?
               JMXConnectorFactory.newJMXConnector(jmxConnectionInfo.jmxServiceURL, null)
                                : JMXConnectorFactory.newJMXConnector(jmxConnectionInfo.jmxServiceURL, envMap);
    }

    private static class PropertiesFile {
        public String username;
        public String password;
        public String keystorePassword;
        public String truststorePassword;
    }
}
