package com.instaclustr.esop.backup;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.Multimap;

import org.apache.commons.lang3.tuple.Pair;

import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.KeyspaceTable.KeyspaceTables;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class KeyspaceTablesTest {

    @Test
    public void testEmpty() {
        KeyspaceTables keyspaceTables = new KeyspaceTables();
        keyspaceTables.add("ks1", "t1");
        keyspaceTables.add("ks2", "t2");
        keyspaceTables.add("ks3", "t3");
        keyspaceTables.add("ks4", "t4");

        DatabaseEntities entities = DatabaseEntities.parse("");

        Optional<Pair<List<String>, Multimap<String, String>>> result = keyspaceTables.filterNotPresent(entities);

        assertFalse(result.isPresent());
    }

    @Test
    public void testMissingTables() {
        KeyspaceTables keyspaceTables = new KeyspaceTables();
        keyspaceTables.add("ks1", "t1");
        keyspaceTables.add("ks2", "t2");
        keyspaceTables.add("ks3", "t3");
        keyspaceTables.add("ks4", "t4");

        DatabaseEntities entities = DatabaseEntities.parse("ks1.t1,ks2.t2,ks5.t5,ks6.t6"); // ks5.t5 and ks6.t6 are missing

        Optional<Pair<List<String>, Multimap<String, String>>> result = keyspaceTables.filterNotPresent(entities);

        assertTrue(result.isPresent());

        Pair<List<String>, Multimap<String, String>> pair = result.get();

        assertEquals(2, pair.getRight().size());
        assertTrue(pair.getRight().containsEntry("ks5", "t5"));
        assertTrue(pair.getRight().containsEntry("ks6", "t6"));
    }

    @Test
    public void testMissingKeyspaces() {
        KeyspaceTables keyspaceTables = new KeyspaceTables();
        keyspaceTables.add("ks1", "t1");
        keyspaceTables.add("ks2", "t2");
        keyspaceTables.add("ks3", "t3");
        keyspaceTables.add("ks4", "t4");

        DatabaseEntities entities = DatabaseEntities.parse("ks1,ks5,ks6"); // ks5 and ks6 are missing

        Optional<Pair<List<String>, Multimap<String, String>>> result = keyspaceTables.filterNotPresent(entities);

        assertTrue(result.isPresent());

        Pair<List<String>, Multimap<String, String>> pair = result.get();

        assertEquals(2, pair.getLeft().size());
        assertTrue(pair.getLeft().contains("ks5"));
        assertTrue(pair.getLeft().contains("ks6"));
    }
}
