package com.instaclustr.cassandra.backup.impl.interaction;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CassandraHostname implements CassandraInteraction<Map<InetAddress, String>> {

    private Set<InetAddress> endpoints;

    public CassandraHostname(final Set<InetAddress> endpoints) {
        this.endpoints = endpoints;
    }

    @Override
    public Map<InetAddress, String> act() throws Exception {
        return endpoints.stream().collect(Collectors.toMap(Function.identity(), InetAddress::getHostName));
    }
}
