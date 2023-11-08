package com.instaclustr.esop.topology;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import jmx.org.apache.cassandra.service.CassandraJMXService;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class CassandraClusterTopology implements CassandraInteraction<ClusterTopology> {

    private static final Logger logger = LoggerFactory.getLogger(CassandraClusterTopology.class);

    private final CassandraJMXService cassandraJMXService;
    private final String dcs;

    public CassandraClusterTopology(final CassandraJMXService cassandraJMXService, final String dcs) {
        this.cassandraJMXService = cassandraJMXService;
        this.dcs = dcs;
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

        final String schemaVersion = new CassandraSchemaVersion(cassandraJMXService).act();

        final ClusterTopology resolvedTopology = constructTopology(clusterName, endpoints, endpointDcs, hostnames, endpointRacks, schemaVersion);

        final Set<String> invalidDcs = ClusterTopology.sanitizeDcs(dcs).stream().filter(dc -> !resolvedTopology.getDcs().contains(dc)).collect(toSet());

        if (!invalidDcs.isEmpty()) {
            throw new IllegalStateException(format("Some DCs to filter on do not exist: %s, existing: %s", invalidDcs, String.join(",", resolvedTopology.getDcs())));
        }

        logger.info(resolvedTopology.toString());

        return resolvedTopology;
    }

    private ClusterTopology constructTopology(final String clusterName,
                                              final Map<InetAddress, UUID> endpoints,
                                              final Map<InetAddress, String> endpointDcs,
                                              final Map<InetAddress, String> hostnames,
                                              final Map<InetAddress, String> endpointRacks,
                                              final String schemaVersion) {
        final ClusterTopology topology = new ClusterTopology();
        topology.setTimestamp(System.currentTimeMillis());

        for (InetAddress inetAddress : endpoints.keySet()) {

            final ClusterTopology.NodeTopology nodeTopology = new ClusterTopology.NodeTopology();

            nodeTopology.setCluster(clusterName);
            nodeTopology.setDc(endpointDcs.get(inetAddress));
            nodeTopology.setNodeId(endpoints.get(inetAddress));
            nodeTopology.setHostname(hostnames.get(inetAddress));
            nodeTopology.setRack(endpointRacks.get(inetAddress));
            nodeTopology.setIpAddress(inetAddress.getHostAddress());

            topology.topology.add(nodeTopology);
        }

        topology.clusterName = clusterName;
        topology.schemaVersion = schemaVersion;
        topology.endpoints = endpoints;
        topology.endpointDcs = endpointDcs;
        topology.hostnames = hostnames;
        topology.endpointRacks = endpointRacks;

        return ClusterTopology.filter(topology, dcs);
    }

    public static class ClusterTopology {

        private static final Logger logger = LoggerFactory.getLogger(ClusterTopology.class);

        public long timestamp;

        public String clusterName;

        @JsonIgnore
        public Map<InetAddress, UUID> endpoints;

        @JsonIgnore
        public Map<InetAddress, String> endpointDcs;

        @JsonIgnore
        public Map<InetAddress, String> hostnames;

        @JsonIgnore
        public Map<InetAddress, String> endpointRacks;

        public String schemaVersion;

        public List<NodeTopology> topology = new ArrayList<>();

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(final long timestamp) {
            this.timestamp = timestamp;
        }

        @JsonIgnore
        public int getClusterSize() {
            return topology.size();
        }

        @JsonIgnore
        public int getNumberOfDcs() {
            return getDcs().size();
        }

        @JsonIgnore
        public Set<String> getDcs() {
            return topology.stream().map(NodeTopology::getDc).collect(toSet());
        }

        @JsonIgnore
        public List<NodeTopology> getNodesFromDcs(final List<String> dcs) {
            return topology.stream().filter(nodeTopology -> dcs.contains(nodeTopology.dc)).collect(toList());
        }

        @JsonIgnore
        public Optional<NodeTopology> getNodeTopology(final String nodeId) {
            return topology.stream().filter(nt -> nt.nodeId.toString().equals(nodeId)).findFirst();
        }

        @JsonIgnore
        public int getNumberOfNodesFromDc(final String dc) {
            return getNodesFromDcs(singletonList(dc)).size();
        }

        @JsonIgnore
        public List<NodeTopology> getNodesFromDcAndRack(final String dc, final String rack) {
            return getNodesFromDcs(singletonList(dc)).stream().filter(nodeTopology -> rack.equals(nodeTopology.rack)).collect(toList());
        }

        @JsonIgnore
        public int getNumberOfNodesFromDcAndRack(final String dc, final String rack) {
            return getNodesFromDcAndRack(dc, rack).size();
        }

        // tailored for Instaclustr Cassandra operator
        public NodeTopology translateToNodeTopology(final String nodeId) {

            logger.info(format("Going to nodeId %s against topology %s", nodeId, this.toString()));

            // Given nodeId is in form "cassandra-test-cluster-dc1-west1-a-0.cassandra-test-cluster-dc1-seeds.default.svc.cluster.local" instead of uuid
            // when we restore by Cassandra operator we need to filter based on "hostname" on nodeTopology instead on "nodeId".
            final List<NodeTopology> nodes = topology.stream()
                .filter(nodeTopology -> nodeTopology.nodeId.toString().equals(nodeId) || nodeTopology.hostname.startsWith(nodeId)).collect(toList());

            if (nodes.size() == 1) {
                return nodes.get(0);
            }

            if (nodes.isEmpty()) {
                throw new IllegalStateException(format("There are no nodes which starts on '%s'", nodeId));
            } else {
                throw new IllegalStateException(format("There are more than 1 nodes which starts on '%s': %s", nodeId, nodes.toString()));
            }
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

        public static void upload(final Backuper backuper,
                                  final ClusterTopology clusterTopology,
                                  final ObjectMapper objectMapper,
                                  final String snapshotTag) throws Exception {
            try {
                final String clusterTopologyString = ClusterTopology.writeToString(objectMapper, clusterTopology);

                final Path topologyPath = Paths.get(format("topology/%s.json", snapshotTag));

                logger.info("Uploading cluster topology under {}", topologyPath);
                logger.info("\n" + clusterTopologyString);

                backuper.uploadText(clusterTopologyString, backuper.objectKeyToRemoteReference(topologyPath));
            } catch (final Exception ex) {
                throw new Exception("Unable to upload cluster topology!", ex);
            }
        }

        public static List<String> sanitizeDcs(final String dcs) {
            if (dcs == null) {
                return Collections.emptyList();
            }
            return Arrays.asList(dcs.trim().replaceAll("[ ]+,", ",").replaceAll(" ", ",").replaceAll("[,]+", ",").split(","));
        }

        public static ClusterTopology filter(final ClusterTopology clusterTopology, final String dc) {
            return filter(clusterTopology, sanitizeDcs(dc));
        }

        public static ClusterTopology filter(final ClusterTopology clusterTopology, final List<String> dcs) {
            if (dcs == null || dcs.isEmpty()) {
                return clusterTopology;
            }

            final List<NodeTopology> nodesFromDc = clusterTopology.getNodesFromDcs(dcs);
            clusterTopology.topology.clear();
            clusterTopology.topology.addAll(nodesFromDc);

            // endpoint dcs
            final Map<InetAddress, String> endpointDcs = clusterTopology.endpointDcs
                .entrySet()
                .stream()
                .filter(entry -> dcs.contains(entry.getValue()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            clusterTopology.endpointDcs.clear();
            clusterTopology.endpointDcs.putAll(endpointDcs);

            // endpoints
            final Map<InetAddress, UUID> endpoints = clusterTopology.endpoints
                .entrySet()
                .stream()
                .filter(entry -> endpointDcs.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            clusterTopology.endpoints.clear();
            clusterTopology.endpoints.putAll(endpoints);

            // hostnames
            final Map<InetAddress, String> hostnames = clusterTopology.hostnames
                .entrySet()
                .stream()
                .filter(entry -> endpointDcs.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            clusterTopology.hostnames.clear();
            clusterTopology.hostnames.putAll(hostnames);

            // endpoint racks
            final Map<InetAddress, String> endpointRacks = clusterTopology.endpointRacks
                .entrySet()
                .stream()
                .filter(entry -> endpointDcs.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            clusterTopology.endpointRacks.clear();
            clusterTopology.endpointRacks.putAll(endpointRacks);

            return clusterTopology;
        }

        @Override
        public String toString() {
            return "ClusterTopology{" +
                "timestamp=" + timestamp +
                ", clusterName='" + clusterName + '\'' +
                ", endpoints=" + endpoints +
                ", endpointDcs=" + endpointDcs +
                ", hostnames=" + hostnames +
                ", endpointRacks=" + endpointRacks +
                ", schemaVersion='" + schemaVersion + '\'' +
                ", topology=" + topology +
                '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ClusterTopology that = (ClusterTopology) o;
            return timestamp == that.timestamp &&
                Objects.equals(clusterName, that.clusterName) &&
                Objects.equals(endpoints, that.endpoints) &&
                Objects.equals(endpointDcs, that.endpointDcs) &&
                Objects.equals(hostnames, that.hostnames) &&
                Objects.equals(endpointRacks, that.endpointRacks) &&
                Objects.equals(schemaVersion, that.schemaVersion) &&
                Objects.equals(topology, that.topology);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, clusterName, endpoints, endpointDcs, hostnames, endpointRacks, schemaVersion, topology);
        }

        public static class NodeTopology {

            public String hostname;

            public String cluster;

            public String dc;

            public String rack;

            public UUID nodeId;

            public String ipAddress;

            public String getHostname() {
                return hostname;
            }

            public void setHostname(final String hostname) {
                this.hostname = hostname;
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

            public UUID getNodeId() {
                return nodeId;
            }

            public void setNodeId(final UUID nodeId) {
                this.nodeId = nodeId;
            }

            public String getIpAddress() {
                return ipAddress;
            }

            public void setIpAddress(final String ipAddress) {
                this.ipAddress = ipAddress;
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                    .add("hostname", hostname)
                    .add("cluster", cluster)
                    .add("dc", dc)
                    .add("rack", rack)
                    .add("nodeId", nodeId)
                    .add("ipAddress", ipAddress)
                    .toString();
            }
        }
    }
}

