package com.instaclustr.cassandra.backup.impl.interaction;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraClusterTopology.ClusterTopology.NodeTopology;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class CassandraClusterTopology implements CassandraInteraction<ClusterTopology> {

    private final CassandraJMXService cassandraJMXService;
    private final String dc;

    public CassandraClusterTopology(final CassandraJMXService cassandraJMXService, final String dc) {
        this.cassandraJMXService = cassandraJMXService;
        this.dc = dc;
    }

    @Override
    public ClusterTopology act() throws Exception {

        final String clusterName = new CassandraClusterName(cassandraJMXService).act();

        // map of endpoints and host ids

        final Map<InetAddress, UUID> endpoints = new CassandraEndpoints(cassandraJMXService).act();

        // map of endpoints and dc they belong to

        final Map<InetAddress, String> endpointDcs = new CassandraEndpointDC(cassandraJMXService, endpoints.keySet()).act();

        // map of endpoints and hostnames

        final Map<InetAddress, String> hostnames = new CassandraHostname(endpoints.keySet()).act();

        // map of endpoints and rack they belong to

        final Map<InetAddress, String> endpointRacks = new CassandraEndpointRack(cassandraJMXService, endpoints.keySet()).act();

        return constructTopology(clusterName, endpoints, endpointDcs, hostnames, endpointRacks);
    }

    private ClusterTopology constructTopology(final String clusterName,
                                              final Map<InetAddress, UUID> endpoints,
                                              final Map<InetAddress, String> endpointDcs,
                                              final Map<InetAddress, String> hostnames,
                                              final Map<InetAddress, String> endpointRacks) {
        final ClusterTopology topology = new ClusterTopology();

        for (InetAddress inetAddress : endpoints.keySet()) {

            final NodeTopology nodeTopology = new NodeTopology();

            nodeTopology.setCluster(clusterName);
            nodeTopology.setDc(endpointDcs.get(inetAddress));
            nodeTopology.setHostId(endpoints.get(inetAddress));
            nodeTopology.setHostname(hostnames.get(inetAddress));
            nodeTopology.setRack(endpointRacks.get(inetAddress));
            nodeTopology.setIpAddress(inetAddress);

            topology.topology.add(nodeTopology);
        }

        return filter(topology, dc);
    }

    public ClusterTopology filter(final ClusterTopology clusterTopology, final String dc) {
        if (dc == null) {
            final ClusterTopology topology = new ClusterTopology();
            topology.topology.addAll(clusterTopology.topology);
            return topology;
        }

        return clusterTopology.filterDc(dc);
    }

    public static class ClusterTopology {

        public List<NodeTopology> topology = new ArrayList<>();

        public int getClusterSize() {
            return topology.size();
        }

        public int getNumberOfDcs() {
            return getDcs().size();
        }

        public Set<String> getDcs() {
            return topology.stream().map(NodeTopology::getDc).collect(toSet());
        }

        public List<NodeTopology> getNodesFromDc(final String dc) {
            return topology.stream().filter(nodeTopology -> dc.equals(nodeTopology.dc)).collect(toList());
        }

        public ClusterTopology filterDc(final String dc) {
            final ClusterTopology clusterTopology = new ClusterTopology();
            clusterTopology.topology.addAll(getNodesFromDc(dc));
            return clusterTopology;
        }

        public int getNumberOfNodesFromDc(final String dc) {
            return getNodesFromDc(dc).size();
        }

        public List<NodeTopology> getNodesFromDcAndRack(final String dc, final String rack) {
            return getNodesFromDc(dc).stream().filter(nodeTopology -> rack.equals(nodeTopology.rack)).collect(toList());
        }

        public int getNumberOfNodesFromDcAndRack(final String dc, final String rack) {
            return getNodesFromDcAndRack(dc, rack).size();
        }

        public static String writeToString(final ObjectMapper objectMapper, final ClusterTopology clusterTopology) throws JsonProcessingException {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(clusterTopology);
        }

        public static void write(final ObjectMapper objectMapper, final ClusterTopology clusterTopology, final Path path) throws IOException {
            write(objectMapper, clusterTopology, path.toFile());
        }

        public static void write(final ObjectMapper objectMapper, final ClusterTopology clusterTopology, final File file) throws IOException {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, clusterTopology);
        }

        public static class NodeTopology {

            public String hostname;

            public String shortHostname;

            public String cluster;

            public String dc;

            public String rack;

            public UUID hostId;

            public InetAddress ipAddress;

            public String getHostname() {
                return hostname;
            }

            public void setHostname(final String hostname) {
                this.hostname = hostname;
            }

            public String getShortHostname() {
                return shortHostname;
            }

            public void setShortHostname(final String shortHostname) {
                this.shortHostname = shortHostname;
            }

            public String getCluster() {
                return cluster;
            }

            public void setCluster(final String cluster) {
                this.cluster = cluster;
            }

            public String getDc() {
                return dc;
            }

            public void setDc(final String dc) {
                this.dc = dc;
            }

            public String getRack() {
                return rack;
            }

            public void setRack(final String rack) {
                this.rack = rack;
            }

            public UUID getHostId() {
                return hostId;
            }

            public void setHostId(final UUID hostId) {
                this.hostId = hostId;
            }

            public InetAddress getIpAddress() {
                return ipAddress;
            }

            public void setIpAddress(final InetAddress ipAddress) {
                this.ipAddress = ipAddress;
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                    .add("hostname", hostname)
                    .add("cluster", cluster)
                    .add("dc", dc)
                    .add("rack", rack)
                    .add("hostId", hostId)
                    .add("ipAddress", ipAddress)
                    .toString();
            }
        }
    }
}
