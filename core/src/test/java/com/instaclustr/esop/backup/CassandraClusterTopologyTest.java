package com.instaclustr.esop.backup;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;

import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import org.junit.Assert;
import org.junit.Test;

import static java.net.InetAddress.getByName;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CassandraClusterTopologyTest {

    private static UUID uuid1 = UUID.randomUUID();
    private static UUID uuid2 = UUID.randomUUID();
    private static UUID uuid3 = UUID.randomUUID();
    private static UUID uuid4 = UUID.randomUUID();
    private static UUID uuid5 = UUID.randomUUID();
    private static UUID uuid6 = UUID.randomUUID();

    private ClusterTopology getClusterTopology() throws Exception {
        ClusterTopology clusterTopology = new ClusterTopology();

        clusterTopology.endpointRacks = new HashMap<InetAddress, String>() {{
            put(getByName("127.0.0.1"), "rack1");
            put(getByName("127.0.0.2"), "rack2");
            put(getByName("127.0.0.3"), "rack3");

            put(getByName("127.0.0.4"), "rack1");
            put(getByName("127.0.0.5"), "rack2");
            put(getByName("127.0.0.6"), "rack3");
        }};

        clusterTopology.hostnames = new HashMap<InetAddress, String>() {{
            put(getByName("127.0.0.1"), "host1");
            put(getByName("127.0.0.2"), "host2");
            put(getByName("127.0.0.3"), "host3");

            put(getByName("127.0.0.4"), "host4");
            put(getByName("127.0.0.5"), "host5");
            put(getByName("127.0.0.6"), "host6");
        }};

        clusterTopology.endpoints = new HashMap<InetAddress, UUID>() {{
            put(getByName("127.0.0.1"), uuid1);
            put(getByName("127.0.0.2"), uuid2);
            put(getByName("127.0.0.3"), uuid3);

            put(getByName("127.0.0.4"), uuid4);
            put(getByName("127.0.0.5"), uuid5);
            put(getByName("127.0.0.6"), uuid6);
        }};

        clusterTopology.endpointDcs = new HashMap<InetAddress, String>() {{
            put(getByName("127.0.0.1"), "dc1");
            put(getByName("127.0.0.2"), "dc1");
            put(getByName("127.0.0.3"), "dc1");

            put(getByName("127.0.0.4"), "dc2");
            put(getByName("127.0.0.5"), "dc2");
            put(getByName("127.0.0.6"), "dc2");
        }};

        clusterTopology.clusterName = "abc";
        clusterTopology.timestamp = System.currentTimeMillis();
        clusterTopology.schemaVersion = UUID.randomUUID().toString();

        return clusterTopology;
    }

    @Test
    public void testNoDcFiltering() throws Exception {
        final ClusterTopology topology = getClusterTopology();
        ClusterTopology filtered = ClusterTopology.filter(topology, Collections.emptyList());
        assertEquals(topology, filtered);
    }

    @Test
    public void testDcFiltering() throws Exception {
        final ClusterTopology topology = getClusterTopology();
        ClusterTopology filtered = ClusterTopology.filter(topology, "dc1");

        Assert.assertEquals(topology.schemaVersion, filtered.schemaVersion);

        Assert.assertEquals(3, topology.endpointDcs.size());
        Assert.assertEquals(3, topology.endpoints.size());
        Assert.assertEquals(3, topology.hostnames.size());
        Assert.assertEquals(3, topology.endpointRacks.size());
    }

    @Test
    public void testMultipleDcFiltering() throws Exception {
        final ClusterTopology topology = getClusterTopology();
        ClusterTopology filtered = ClusterTopology.filter(topology, " dc1,   dc2   ,");

        Assert.assertEquals(topology.schemaVersion, filtered.schemaVersion);

        Assert.assertEquals(6, topology.endpointDcs.size());
        Assert.assertEquals(6, topology.endpoints.size());
        Assert.assertEquals(6, topology.hostnames.size());
        Assert.assertEquals(6, topology.endpointRacks.size());
    }

    @Test
    public void invalidDcs() throws Exception {
        final ClusterTopology topology = getClusterTopology();
        final Set<String> invalidDcs = ClusterTopology.sanitizeDcs(" dc3   ,").stream().filter(dc -> !topology.getDcs().contains(dc)).collect(toSet());
        assertFalse(invalidDcs.isEmpty());
    }
}
