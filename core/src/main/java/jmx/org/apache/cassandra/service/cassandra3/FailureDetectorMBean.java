package jmx.org.apache.cassandra.service.cassandra3;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import java.net.UnknownHostException;
import java.util.Map;

public interface FailureDetectorMBean {

    public void dumpInterArrivalTimes();

    public void setPhiConvictThreshold(double phi);

    public double getPhiConvictThreshold();

    public String getAllEndpointStates();

    public String getEndpointState(String address) throws UnknownHostException;

    public Map<String, String> getSimpleStates();

    public int getDownEndpointCount();

    public int getUpEndpointCount();

    public TabularData getPhiValues() throws OpenDataException;
}
