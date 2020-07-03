package com.instaclustr.cassandra.backup.impl.interaction;

import static java.nio.file.Files.readAllLines;
import static java.util.Arrays.asList;

import java.nio.file.Path;
import java.util.List;

import com.instaclustr.cassandra.CassandraInteraction;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class CassandraSameTokens implements CassandraInteraction<Boolean> {

    private final CassandraJMXService cassandraJMXService;
    private final Path tokenFile;

    public CassandraSameTokens(final CassandraJMXService cassandraJMXService,
                               final Path tokenFile) {
        this.cassandraJMXService = cassandraJMXService;
        this.tokenFile = tokenFile;
    }

    @Override
    public Boolean act() throws Exception {
        final List<String> tokensFromFile = tokensFromFile();
        final List<String> tokensOfNode = new CassandraTokens(cassandraJMXService).act();

        return tokensFromFile.size() == tokensOfNode.size() && tokensFromFile.containsAll(tokensOfNode);
    }

    private List<String> tokensFromFile() throws Exception {
        return asList(readAllLines(tokenFile)
                          .stream()
                          .filter(line -> !line.trim().isEmpty() && line.trim().startsWith("initial_token:"))
                          .map(line -> line.trim().split(" ")[1])
                          .findFirst().orElseThrow(() -> new IllegalStateException("Malformat of initial_token line in tokens file."))
                          .split(","));
    }
}
