package com.instaclustr.esop.backup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.instaclustr.esop.impl.CassandraData;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.RenamedEntities;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class CassandraDataTest {

    final Map<String, Map<String, String>> tableIdsMap = new HashMap<String, Map<String, String>>() {{
        put("system_schema", new HashMap<String, String>() {{
            put("tables", "999999999999999999999999999999999");
            put("columns", "000000000000000000000000000000000");
        }});
        put("ks1", new HashMap<String, String>() {{
            put("tb1", "11111111111111111111111111111111");
            put("tb2", "22222222222222222222222222222222");
            put("tb3", "33333333333333333333333333333333");
            put("tb4", "44444444444444444444444444444444");
        }});
        put("ks2", new HashMap<String, String>() {{
            put("tb1", "55555555555555555555555555555555");
            put("tb2", "66666666666666666666666666666666");
            put("tb3", "77777777777777777777777777777777");
            put("tb4", "88888888888888888888888888888888");
        }});
    }};

    final Map<Path, List<Path>> paths = new HashMap<Path, List<Path>>() {{
        put(Paths.get("/var/lib/cassandra/data/data/system_schema"), Arrays.asList(
            Paths.get("/var/lib/cassandra/data/data/ks1/tables-99999999999999999999999999999999"),
            Paths.get("/var/lib/cassandra/data/data/ks1/columns-00000000000000000000000000000000")
        ));
        put(Paths.get("/var/lib/cassandra/data/data/ks1"), Arrays.asList(
            Paths.get("/var/lib/cassandra/data/data/ks1/tb1-11111111111111111111111111111111"),
            Paths.get("/var/lib/cassandra/data/data/ks1/tb2-22222222222222222222222222222222"),
            Paths.get("/var/lib/cassandra/data/data/ks1/tb3-33333333333333333333333333333333"),
            Paths.get("/var/lib/cassandra/data/data/ks1/tb4-44444444444444444444444444444444")
        ));
        put(Paths.get("/var/lib/cassandra/data/data/ks2"), Arrays.asList(
            Paths.get("/var/lib/cassandra/data/data/ks2/tb1-55555555555555555555555555555555"),
            Paths.get("/var/lib/cassandra/data/data/ks2/tb2-66666666666666666666666666666666"),
            Paths.get("/var/lib/cassandra/data/data/ks2/tb3-77777777777777777777777777777777"),
            Paths.get("/var/lib/cassandra/data/data/ks2/tb4-88888888888888888888888888888888")
        ));
    }};

    @Test
    public void testCassandraData() {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        assertTrue(parsed.getTableId("system_schema", "tables").isPresent());
        assertFalse(parsed.getTableId("system_schema", "asdadsad").isPresent());
        assertFalse(parsed.getTableId("sdadasd", "types").isPresent());

        Optional<List<String>> systemSchemaTablesNames = parsed.getTablesNames("system_schema");
        assertTrue(systemSchemaTablesNames.isPresent());

        Optional<List<Path>> systemSchemaTablesPaths = parsed.getTablesPaths("system_schema");
        assertTrue(systemSchemaTablesPaths.isPresent());
        Assert.assertEquals(2, systemSchemaTablesPaths.get().size());

        assertTrue(parsed.containsKeyspace("ks1"));
        assertFalse(parsed.containsKeyspace("asdsad"));

        assertTrue(parsed.containsTable("ks1", "tb1"));
        assertTrue(parsed.containsTable("ks1", "tb2"));
        assertFalse(parsed.containsTable("ks1", "sdada"));

        assertEquals("11111111111111111111111111111111", parsed.getTableId("ks1", "tb1").get());
        assertEquals("88888888888888888888888888888888", parsed.getTableId("ks2", "tb4").get());
        assertFalse(parsed.getTableId("ks2", "sdadsad").isPresent());

        assertTrue(parsed.getTablesNames("ks1").get().containsAll(Arrays.asList("tb1", "tb2", "tb3", "tb4")));
        assertTrue(parsed.getTablesNames("ks2").get().containsAll(Arrays.asList("tb1", "tb2", "tb3", "tb4")));

        assertTrue(parsed.getTablesPaths("ks1").get().containsAll(Arrays.asList(
            Paths.get("/var/lib/cassandra/data/data/ks1/tb1-11111111111111111111111111111111"),
            Paths.get("/var/lib/cassandra/data/data/ks1/tb2-22222222222222222222222222222222"),
            Paths.get("/var/lib/cassandra/data/data/ks1/tb3-33333333333333333333333333333333"),
            Paths.get("/var/lib/cassandra/data/data/ks1/tb4-44444444444444444444444444444444")
        )));

        assertTrue(parsed.getTablePath("ks1", "tb3").get().equals(Paths.get("/var/lib/cassandra/data/data/ks1/tb3-33333333333333333333333333333333")));
        assertFalse(parsed.getTablePath("ks1", "sdad").isPresent());
        assertFalse(parsed.getTablePath("sdad", "sdad").isPresent());

        assertEquals(Paths.get("/var/lib/cassandra/data/data/ks1"), parsed.getKeyspacePath("ks1").get());

        assertEquals(3, parsed.getKeyspacePaths().size());

        // database entities

        DatabaseEntities databaseEntities = parsed.toDatabaseEntities(false);
        Assert.assertFalse(databaseEntities.areEmpty());
        assertTrue(databaseEntities.getKeyspaces().containsAll(Arrays.asList("ks1", "ks2")));
        databaseEntities.getKeyspacesAndTables().entries().forEach(entry -> {
            Assert.assertTrue(parsed.getTableId(entry.getKey(), entry.getValue()).isPresent());
        });

        assertFalse(databaseEntities.contains("system_schema"));
        assertFalse(databaseEntities.contains("asdasda", "asdad"));
    }

    @Test
    public void testRenamedEntitiesWithDatabaseEntities() {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        DatabaseEntities entities = DatabaseEntities.parse("ks1.tb1,ks1.tb2");

        parsed.setDatabaseEntitiesFromRequest(entities);

        parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb3");
        }});
    }

    @Test
    public void testDatabaseEntities() throws Exception {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        DatabaseEntities entities = DatabaseEntities.parse("ks1.tb1,kb3.tb3");

        try {
            parsed.setDatabaseEntitiesFromRequest(entities);
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "Tables [kb3.tb3] to process are not present in Cassandra.");
        }

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1.tb1"));
    }

    @Test
    public void testRenamedEntities() throws Exception {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        try {
            parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
                put("ks3.tb4", "ks1.tb2");
            }});

            Assert.fail("should fail on non-existing ks3.tb4");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "There is not keyspace ks3 to rename an entity from!");
        }

        try {
            parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
                put("ks1.tb5", "ks1.tb2");
            }});

            Assert.fail("should fail on non-existing ks1.tb5");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "There is not table ks1.tb5 to rename an entity from!");
        }

        try {
            parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
                put("ks1.tb1", "ks3.tb1");
            }});

            Assert.fail("should fail on non-existing ks3.tb1");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "There is not keyspace ks3 to rename an entity to!");
        }

        try {
            parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
                put("ks1.tb1", "ks2.tb5");
            }});

            Assert.fail("should fail on non-existing ks2.tb5");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "There is not table ks2.tb5 to rename an entity to!");
        }
    }

    private ObjectMapper objectMapper;
    private Manifest manifest;

    @BeforeTest
    public void before() throws Exception {
        objectMapper = new ObjectMapper();

        objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_ABSENT);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.disable(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS);
        objectMapper.writerWithDefaultPrettyPrinter();

        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new JavaTimeModule());

        manifest = Manifest.read(Paths.get("src/test/resources/cassandra-data-test-manifest.json"), objectMapper);
    }

    @Test
    public void testGetEntitiesToTruncate() {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1.tb1"));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb3");
        }}));

        DatabaseEntities databaseEntitiesToTruncate = parsed.getDatabaseEntitiesToProcessForRestore(manifest);

        Assert.assertEquals(1, databaseEntitiesToTruncate.getKeyspaces().size());
        Assert.assertEquals("ks1", databaseEntitiesToTruncate.getKeyspaces().get(0));

        Assert.assertEquals(2, databaseEntitiesToTruncate.getKeyspacesAndTables().size());
        Assert.assertTrue(databaseEntitiesToTruncate.getKeyspacesAndTables().containsEntry("ks1", "tb1"));
        Assert.assertFalse(databaseEntitiesToTruncate.getKeyspacesAndTables().containsEntry("ks1", "tb2"));
        Assert.assertTrue(databaseEntitiesToTruncate.getKeyspacesAndTables().containsEntry("ks1", "tb3"));
    }

    @Test
    public void testGetEntitiesToTruncate2() throws Exception {
        Manifest manifest = Manifest.read(Paths.get("src/test/resources/cassandra-data-test-manifest.json"), objectMapper);
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1.tb1,ks1.tb2"));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb3");
        }}));

        DatabaseEntities toProcess = parsed.getDatabaseEntitiesToProcessForRestore(manifest);

        Assert.assertEquals(1, toProcess.getKeyspaces().size());
        Assert.assertEquals("ks1", toProcess.getKeyspaces().get(0));

        Assert.assertEquals(2, toProcess.getKeyspacesAndTables().size());
        Assert.assertTrue(toProcess.getKeyspacesAndTables().containsEntry("ks1", "tb1"));
        Assert.assertFalse(toProcess.getKeyspacesAndTables().containsEntry("ks1", "tb2"));
        Assert.assertTrue(toProcess.getKeyspacesAndTables().containsEntry("ks1", "tb3"));

        System.out.println(toProcess);
    }

    @Test
    public void testGetEntitiesToTruncate3() {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1"));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb3");
        }}));

        DatabaseEntities databaseEntitiesToTruncate = parsed.getDatabaseEntitiesToProcessForRestore(manifest);

        Assert.assertEquals(1, databaseEntitiesToTruncate.getKeyspaces().size());
        Assert.assertEquals("ks1", databaseEntitiesToTruncate.getKeyspaces().get(0));

        Assert.assertEquals(3, databaseEntitiesToTruncate.getKeyspacesAndTables().size());
        Assert.assertTrue(databaseEntitiesToTruncate.getKeyspacesAndTables().containsEntry("ks1", "tb1"));
        Assert.assertFalse(databaseEntitiesToTruncate.getKeyspacesAndTables().containsEntry("ks1", "tb2"));
        Assert.assertTrue(databaseEntitiesToTruncate.getKeyspacesAndTables().containsEntry("ks1", "tb3"));
        Assert.assertTrue(databaseEntitiesToTruncate.getKeyspacesAndTables().containsEntry("ks1", "tb4"));

        System.out.println(databaseEntitiesToTruncate);
    }

    @Test
    public void testGetEntitiesToTruncate4() {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse(""));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb3");
            put("ks2.tb3", "ks2.tb4");
        }}));

        DatabaseEntities toProcess = parsed.getDatabaseEntitiesToProcessForRestore(manifest);

        Assert.assertEquals(2, toProcess.getKeyspaces().size());
        Assert.assertEquals("ks1", toProcess.getKeyspaces().get(0));
        Assert.assertEquals("ks2", toProcess.getKeyspaces().get(1));

        Assert.assertEquals(6, toProcess.getKeyspacesAndTables().size());

        // ks1
        Assert.assertTrue(toProcess.getKeyspacesAndTables().containsEntry("ks1", "tb1"));

        Assert.assertFalse(toProcess.getKeyspacesAndTables().containsEntry("ks1", "tb2"));

        Assert.assertTrue(toProcess.getKeyspacesAndTables().containsEntry("ks1", "tb3"));
        Assert.assertTrue(toProcess.getKeyspacesAndTables().containsEntry("ks1", "tb4"));

        // ks2

        Assert.assertTrue(toProcess.getKeyspacesAndTables().containsEntry("ks2", "tb1"));
        Assert.assertTrue(toProcess.getKeyspacesAndTables().containsEntry("ks2", "tb2"));

        Assert.assertFalse(toProcess.getKeyspacesAndTables().containsEntry("ks2", "tb3"));

        Assert.assertTrue(toProcess.getKeyspacesAndTables().containsEntry("ks2", "tb4"));
    }

    @Test
    public void testRenamed() {
        try {
            RenamedEntities.validate(new HashMap<String, String>() {{
                put("ks1.tb1", "ks2.tb2");
            }});

            Assert.fail("should error out on different keyspace");
        } catch (final Exception ex) {
            // empty
        }

        try {
            RenamedEntities.validate(new HashMap<String, String>() {{
                put("ks1.tb1", "ks1.tb1");
                put("ks2.tb2", "ks1.tb1");
            }});

            Assert.fail("should error out on non-distinct values");
        } catch (final Exception ex) {
            // empty
        }
    }
}