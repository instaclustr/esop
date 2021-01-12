package com.instaclustr.esop.impl;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.instaclustr.esop.impl.RenamedEntities.Renamed.To;

public class RenamedEntities {

    private final List<Renamed> renamed = new ArrayList<>();

    private RenamedEntities() {

    }

    public static RenamedEntities empty() {
        return new RenamedEntities();
    }

    public static RenamedEntities parse(final Map<String, String> pairs) {
        if (pairs == null || pairs.isEmpty()) {
            return RenamedEntities.empty();
        }
        return new RenamedEntities(pairs.entrySet().stream().map(Renamed::new).collect(toList()));
    }

    public RenamedEntities(final List<Renamed> renamed) {
        this.renamed.addAll(renamed);
    }

    public static void validate(final Map<String, String> rename) throws Exception {
        if (rename == null) {
            return;
        }
        final RenamedEntities parsed = RenamedEntities.parse(rename);

        for (final Renamed renamed : parsed.getRenamed()) {
            // source and target keyspace have to be same
            if (!renamed.from.fromKeyspace.equals(renamed.to.toKeyspace)) {
                throw new IllegalStateException("'from' and 'to' keyspace part for an entry in rename map has to be equal");
            }
        }

        if (rename.size() != rename.values().stream().distinct().count()) {
            throw new IllegalStateException("all values in rename map have to be distinct");
        }
    }

    public List<Renamed> getRenamed() {
        return Collections.unmodifiableList(renamed);
    }

    public Optional<To> getRenamedTo(final String keyspace, final String table) {
        return renamed.stream()
            .filter(r -> r.from.fromKeyspace.equals(keyspace) && r.from.fromTable.equals(table))
            .findFirst()
            .map(r -> r.to);
    }

    public boolean isRenamed(final String keyspace, final String table) {
        return getRenamedTo(keyspace, table).isPresent();
    }

    @Override
    public String toString() {
        return renamed.stream().map(Renamed::toString).collect(Collectors.joining(","));
    }

    public static class Renamed {

        public final From from;
        public final To to;

        public static class From {

            public final String fromKeyspace;
            public final String fromTable;

            public From(String keyspace, String table) {
                this.fromKeyspace = keyspace;
                this.fromTable = table;
            }

            @Override
            public String toString() {
                return String.format("%s.%s", fromKeyspace, fromTable);
            }
        }

        public static class To {

            public final String toKeyspace;
            public final String toTable;

            public To(String keyspace, String table) {
                this.toKeyspace = keyspace;
                this.toTable = table;
            }

            @Override
            public String toString() {
                return String.format("%s.%s", toKeyspace, toTable);
            }
        }

        @Override
        public String toString() {
            return String.format("[%s->%s]", from, to);
        }

        public Renamed(String fromKeyspace, String fromTable, String toKeyspace, String toTable) {
            this.from = new From(fromKeyspace, fromTable);
            this.to = new To(toKeyspace, toTable);
        }

        public Renamed(String fromKeyspaceTable, String toKeyspaceTable) {
            if (fromKeyspaceTable.equals(toKeyspaceTable)) {
                throw new IllegalStateException("Can not rename table " + fromKeyspaceTable + " to itself!");
            }

            final String[] fromSplit = fromKeyspaceTable.split("\\.");
            final String[] toSplit = toKeyspaceTable.split("\\.");

            if (fromSplit.length != 2) {
                throw new IllegalStateException("from side is not in format {keyspace}.{table}");
            }

            if (toSplit.length != 2) {
                throw new IllegalStateException("to side is not in format {keyspace}.{table}");
            }

            this.from = new From(fromSplit[0], fromSplit[1]);
            this.to = new To(toSplit[0], toSplit[1]);
        }

        public Renamed(Map.Entry<String, String> pair) {
            this(pair.getKey(), pair.getValue());
        }
    }

}
