package apoc.export.cypher;

import apoc.bolt.BoltTest;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;

import static apoc.export.cypher.ExportCypherResults.*;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {

    private static final Map<String, Object> exportConfig = Collections.singletonMap("separateFiles", true);
    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true").newGraphDatabase();
        TestUtil.registerProcedure(db, ExportCypher.class, Graphs.class);
        db.execute("CREATE INDEX ON :Foo(name)").close();
        db.execute("CREATE INDEX ON :Bar(first_name, last_name)").close();
        db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
        db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})").close();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }


    @Test public void testExportAllCypherResults() {
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null,null)", (r) -> {
            assertResults(null, r, "database");
            assertEquals(EXPECTED_NEO4J_SHELL,r.get("cypherStatements"));
        });
    }

    @Test public void testExportAllCypherStreaming() {
        StringBuilder sb=new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.cypher.all(null,{streamStatements:true,batchSize:3})", (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(5L, r.get("properties"));
            assertNull(r.get("file"));
            assertEquals("cypher", r.get("format"));
            assertTrue(((long) r.get("time")) >= 0);
            sb.append(r.get("cypherStatements"));
            r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(1L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertTrue(((long) r.get("time")) >= 0);
            sb.append(r.get("cypherStatements"));
        });
        assertEquals(EXPECTED_NEO4J_SHELL.replace("LIMIT 20000","LIMIT 3"),sb.toString());
    }

    // -- Whole file test -- //
    @Test public void testExportAllCypherDefault() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},null)", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    @Test public void testExportAllCypherForCypherShell() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{config})",
                map("file", output.getAbsolutePath(), "config", Util.map("format", "cypher-shell")), (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL, readFile(output));
    }

    @Test public void testExportQueryCypherForNeo4j() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    private static String readFile(File output) throws FileNotFoundException {
        return new Scanner(output).useDelimiter("\\Z").next() + String.format("%n");
    }

    @Test public void testExportGraphCypher() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},null) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    // -- Separate files tests -- //
    @Test public void testExportAllCypherNodes() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NODES, readFile(new File(directory, "all.nodes.cypher")));
    }

    @Test public void testExportAllCypherRelationships() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile(new File(directory, "all.relationships.cypher")));
    }

    @Test public void testExportAllCypherSchema() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_SCHEMA, readFile(new File(directory, "all.schema.cypher")));
    }

    @Test public void testExportAllCypherCleanUp() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_CLEAN_UP, readFile(new File(directory, "all.cleanup.cypher")));
    }

    @Test public void testExportGraphCypherNodes() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig), (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NODES, readFile(new File(directory, "graph.nodes.cypher")));
    }

    @Test public void testExportGraphCypherRelationships() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile(new File(directory, "graph.relationships.cypher")));
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_SCHEMA, readFile(new File(directory, "graph.schema.cypher")));
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_CLEAN_UP, readFile(new File(directory, "graph.cleanup.cypher")));
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(6L, r.get("properties"));
        assertEquals(output==null ? null : output.getAbsolutePath(), r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue(((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherPlainFormat() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "plain")), (r) -> {
                });
        assertEquals(EXPECTED_PLAIN, readFile(output));
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell","cypherFormat","updateAll")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_MERGE, readFile(output));
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell","cypherFormat","addStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY, readFile(output));
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell","cypherFormat","updateStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY, readFile(output));
    }

    @Test public void testExportSchemaCypher() throws Exception {
        File output = new File(directory, "onlySchema.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig), (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL, readFile(new File(directory, "onlySchema.cypher")));
    }

    @Test public void testExportSchemaCypherShell() throws Exception {
        File output = new File(directory, "onlySchema.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", Util.map("format", "cypher-shell")), (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, readFile(new File(directory, "onlySchema.cypher")));
    }

    @Test
    public void testExportCypherNodePoint() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo'," +
                "place2d:point({ x: 2.3, y: 4.5 })," +
                "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})" +
                "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->" +
                "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})").close();
        File output = new File(directory, "temporalPoint.cypher");
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_POINT, readFile(output));
    }

    @Test
    public void testExportCypherNodeDate() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "date:date('2018-10-30'), " +
                "datetime:datetime('2018-10-30T12:50:35.556+0100'), " +
                "localTime:localdatetime('20181030T19:32:24')})" +
                "-[:FRIEND_OF {date:date('2018-10-30')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})").close();
        File output = new File(directory, "temporalDate.cypher");
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_DATE, readFile(output));
    }

    @Test
    public void testExportCypherNodeTime() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "local:localtime('12:50:35.556')," +
                "t:time('125035.556+0100')})" +
                "-[:FRIEND_OF {t:time('125035.556+0100')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})").close();
        File output = new File(directory, "temporalTime.cypher");
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_TIME, readFile(output));
    }

    @Test
    public void testExportCypherNodeDuration() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "duration:duration('P5M1.5D')})" +
                "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->" +
                "(:Bar {duration:duration('P5M1.5D')})").close();
        File output = new File(directory, "temporalDuration.cypher");
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_DURATION, readFile(output));
    }

}
