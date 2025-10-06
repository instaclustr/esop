package jmx.org.apache.cassandra.service.cassandra4;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import java.net.UnknownHostException;
import java.util.Map;

public interface FailureDetectorMBean {

    public void dumpInterArrivalTimes();

    public void setPhiConvictThreshold(double phi);

    public double getPhiConvictThreshold();

    @Deprecated
    public String getAllEndpointStates();

    public String getAllEndpointStatesWithPort();

    public String getEndpointState(String address) throws UnknownHostException;

    @Deprecated
    public Map<String, String> getSimpleStates();

    public Map<String, String> getSimpleStatesWithPort();

    public int getDownEndpointCount();

    public int getUpEndpointCount();

    @Deprecated
    public TabularData getPhiValues() throws OpenDataException;

    public TabularData getPhiValuesWithPort() throws OpenDataException;
}

