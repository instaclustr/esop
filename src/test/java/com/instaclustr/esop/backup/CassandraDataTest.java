package com.instaclustr.esop.backup;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
        assertEquals(2, systemSchemaTablesPaths.get().size());

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
        assertFalse(databaseEntities.areEmpty());
        assertTrue(databaseEntities.getKeyspaces().containsAll(Arrays.asList("ks1", "ks2")));
        databaseEntities.getKeyspacesAndTables().entries().forEach(entry -> {
            assertTrue(parsed.getTableId(entry.getKey(), entry.getValue()).isPresent());
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
    public void testDatabaseEntitiesOnSpacesOnly() throws Exception {

        for (String entity : Stream.of("  ks1  .tb1  ks3.    tb3  ",
                                       "ks1.tb1  ks3.tb3",
                                       "  ks1 .tb1  ks3.tb3  ",
                                       " ks1.tb1  ks3.tb3,  ",
                                       " ks1.tb1,ks3.tb3",
                                       ",,,ks1.tb1,,,ks3.tb3,,,").collect(Collectors.toList())) {
            DatabaseEntities parsed = DatabaseEntities.parse(entity);

            Assert.assertTrue(parsed.getKeyspacesAndTables().containsEntry("ks1", "tb1"), entity);
            Assert.assertTrue(parsed.getKeyspacesAndTables().containsEntry("ks3", "tb3"), entity);
            Assert.assertEquals(parsed.getKeyspacesAndTables().size(), 2, entity);
        }

        for (String entity : Stream.of("  ks1  ks3  ",
                                       "ks1 ks3",
                                       " ks1,ks3,   ,",
                                       ",,,ks1,ks3,,,").collect(Collectors.toList())) {
            DatabaseEntities parsed = DatabaseEntities.parse(entity);

            Assert.assertTrue(parsed.getKeyspaces().contains("ks1"), entity);
            Assert.assertTrue(parsed.getKeyspaces().contains("ks3"), entity);
            Assert.assertEquals(parsed.getKeyspaces().size(), 2, entity);
        }
    }

    @Test
    public void testDatabaseEntities() throws Exception {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        DatabaseEntities entities = DatabaseEntities.parse("ks1  .tb1  ,  kb3.  tb3");

        try {
            parsed.setDatabaseEntitiesFromRequest(entities);
        } catch (Exception ex) {
            assertEquals(ex.getMessage(), "Tables [kb3.tb3] to process are not present in Cassandra.");
        }

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1  .tb1"));
    }

    @Test
    public void testRenamedEntities() throws Exception {
        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        try {
            parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
                put("ks3.  tb4", "ks1.  tb2");
            }});

            Assert.fail("should fail on non-existing ks3.tb4");
        } catch (Exception ex) {
            assertEquals(ex.getMessage(), "There is not keyspace ks3 to rename an entity from!");
        }

        try {
            parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
                put("ks1.tb5", "ks1.tb2");
            }});

            Assert.fail("should fail on non-existing ks1.tb5");
        } catch (Exception ex) {
            assertEquals(ex.getMessage(), "There is not table ks1.tb5 to rename an entity from!");
        }

        try {
            parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
                put("ks1.  tb1", "ks3  . tb1");
            }});

            Assert.fail("should fail on non-existing ks3.tb1");
        } catch (Exception ex) {
            assertEquals(ex.getMessage(), "There is not keyspace ks3 to rename an entity to!");
        }

        try {
            parsed.setRenamedEntitiesFromRequest(new HashMap<String, String>() {{
                put("ks1   .tb1", "ks2.  tb5");
            }});

            Assert.fail("should fail on non-existing ks2.tb5");
        } catch (Exception ex) {
            assertEquals(ex.getMessage(), "There is not table ks2.tb5 to rename an entity to!");
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
    // --entities="" --rename=whatever non empty  -> invalid
    public void testCassandraDataEntitiesEmptyRenameNonEmptySholdFail() {

        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse(""));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb3");
        }}));

        try {
            parsed.validate();
            fail("setting rename entities but not setting entities should fail");
        } catch (final Exception ex) {
            // ok
        }
    }

    @Test
    // --entities=ks1 --rename=whatever non empty -> invalid
    public void testCassandraDataKeyspaceEntitiesNotEmptyRenameNonEmptySholdFail() {

        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1"));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb3");
        }}));

        try {
            parsed.validate();
            fail("setting rename entities with set keyspace entities should fail");
        } catch (final Exception ex) {
            // ok
        }
    }

    @Test
    // --entities=ks1.tb1 --rename=ks1.tb2=ks1.tb2 -> invalid as "from" is not in entities
    public void testCassandraDataMissingFromInRenamingInEntitiesShouldFail() {

        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1  .  tb1"));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb3");
        }}));

        try {
            parsed.validate();
            fail("setting rename entities where 'from' is missing in entities should fail");
        } catch (final Exception ex) {
            // ok
        }
    }

    @Test
    // --entities=ks1.tb1,ks1.tb2 --rename=ks1.tb2=ks1.tb1 -> invalid as "to" is in entities
    public void testCassandraDataToInRenamingEntitiesIsPresentInEntitiesShouldFail() {

        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1.tb1,   ks1.tb2"));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb2", "ks1.tb1");
        }}));

        try {
            parsed.validate();
            fail("setting rename entities where 'to' is present in entities should fail");
        } catch (final Exception ex) {
            // ok
        }
    }

    @Test
    // --entities=ks1.tb1,ks1.tb2 --rename=ks1.tb2=ks1.tb1 -> invalid as "to" is in entities
    public void testCassandraDataForRenamingEntities() {

        CassandraData parsed = new CassandraData(tableIdsMap, paths);

        parsed.setDatabaseEntitiesFromRequest(DatabaseEntities.parse("ks1.tb1,  ks1.tb3,   ks2.tb3,   ks2.tb4"));

        parsed.setRenamedEntitiesFromRequest(RenamedEntities.parse(new HashMap<String, String>() {{
            put("ks1.tb1", "ks1.tb2");
            put("ks2.tb3", "ks2.tb2");
        }}));

        DatabaseEntities databaseEntitiesToProcessForRestore = parsed.getDatabaseEntitiesToProcessForRestore();

        assertTrue(databaseEntitiesToProcessForRestore.contains("ks1", "tb2"));
        assertTrue(databaseEntitiesToProcessForRestore.contains("ks2", "tb2"));
        assertTrue(databaseEntitiesToProcessForRestore.contains("ks1", "tb3"));
        assertTrue(databaseEntitiesToProcessForRestore.contains("ks2", "tb4"));

        assertFalse(databaseEntitiesToProcessForRestore.contains("ks1", "tb1"));
        assertFalse(databaseEntitiesToProcessForRestore.contains("ks2", "tb3"));

        assertEquals(2, databaseEntitiesToProcessForRestore.getKeyspaces().size());
        assertEquals(4, databaseEntitiesToProcessForRestore.getKeyspacesAndTables().size());
    }

    @Test
    public void testRenamed() {
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