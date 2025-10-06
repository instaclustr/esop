package jmx.org.apache.cassandra.service.cassandra3;

import java.net.UnknownHostException;

public interface EndpointSnitchInfoMBean {

    /**
     * Provides the Rack name depending on the respective snitch used, given the host name/ip
     * @param host
     * @throws UnknownHostException
     */
    public String getRack(String host) throws UnknownHostException;

    /**
     * Provides the Datacenter name depending on the respective snitch used, given the hostname/ip
     * @param host
     * @throws UnknownHostException
     */
    public String getDatacenter(String host) throws UnknownHostException;

    /**
     * Provides the Rack name depending on the respective snitch used for this node
     */
    public String getRack();

    /**
     * Provides the Datacenter name depending on the respective snitch used for this node
     */
    public String getDatacenter();

    /**
     * Provides the snitch name of the cluster
     * @return Snitch name
     */
    public String getSnitchName();
}
