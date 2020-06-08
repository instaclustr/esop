package com.instaclustr.cassandra.backup.impl.interaction;

import java.util.List;

import com.instaclustr.cassandra.CassandraInteraction;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class CassandraSameTokens implements CassandraInteraction<Boolean> {

    private final CassandraJMXService cassandraJMXService;
    private final List<String> tokens;

    public CassandraSameTokens(final CassandraJMXService cassandraJMXService,
                               final List<String> tokens) {
        this.cassandraJMXService = cassandraJMXService;
        this.tokens = tokens;
    }

    @Override
    public Boolean act() throws Exception {
        final List<String> tokensOfNode = new CassandraTokens(cassandraJMXService).act();
        if (!(tokens.size() == tokensOfNode.size() && tokens.containsAll(tokensOfNode))) {
            throw new IllegalStateException("Tokens from snapshot and tokens of this node does not match!");
        }

        return true;
    }
}
