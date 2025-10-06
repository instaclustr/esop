package com.instaclustr.picocli;

import static com.instaclustr.picocli.typeconverter.CassandraJMXServiceURLTypeConverter.DEFAULT_CASSANDRA_JMX_PORT;

import javax.management.remote.JMXServiceURL;

import com.instaclustr.picocli.typeconverter.CassandraJMXServiceURLTypeConverter;
import picocli.CommandLine.Option;

/**
 * Holds JMX connection information, used as mixin for application to achieve DRY.
 */
public class CassandraJMXSpec {

    @Option(names = "--jmx-service",
            paramLabel = "[ADDRESS][:PORT]|[JMX SERVICE URL]",
            defaultValue = ":" + DEFAULT_CASSANDRA_JMX_PORT,
            converter = CassandraJMXServiceURLTypeConverter.class,
            description = "Address (and optional port) of a Cassandra instance to connect to via JMX. " +
                    "ADDRESS may be a hostname, IPv4 dotted or decimal address, or IPv6 address. " +
                    "When ADDRESS is omitted, the loopback address is used. " +
                    "PORT, when specified, must be a valid port number. The default port " + DEFAULT_CASSANDRA_JMX_PORT + " will be substituted if omitted." +
                    "Defaults to '${DEFAULT-VALUE}'")
    public JMXServiceURL jmxServiceURL;

    @Option(names = "--jmx-user", paramLabel = "[STRING]", description = "User for JMX for Cassandra")
    public String jmxUser;

    @Option(names = "--jmx-password", paramLabel = "[STRING]", description = "Password for JMX for Cassandra")
    public String jmxPassword;

    @Option(names = "--jmx-credentials", paramLabel = "[PATH]", description = "Path to file with username and password for JMX connection")
    public String jmxCredentials;

    @Option(names = "--jmx-truststore", paramLabel = "[PATH]", description = "Path to truststore file for Cassandra")
    public String trustStore;

    @Option(names = "--jmx-truststore-password", paramLabel = "[PATH]", description = "Password to truststore file for Cassandra")
    public String trustStorePassword;

    @Option(names = "--jmx-keystore", paramLabel = "[STRING]", description = "Path to keystore file for Cassandra")
    public String keyStore;

    @Option(names = "--jmx-keystore-password", paramLabel = "[STRING]", description = "Password to keystore file for Cassandra")
    public String keyStorePassword;

    @Option(names = "--jmx-client-auth", description = "boolean saying if c.s.m.j.ssl.need.client.auth should be set, defaults to false")
    public boolean jmxClientAuth;
}
