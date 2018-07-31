package apoc.export.json;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.Map;
import java.util.Scanner;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;

public class ExportJsonTest {

    private static final String EXPECTED_QUERY_NODES = String.format("[ {\n" +
            "  \"id\" : 0,\n" +
            "  \"labels\" : [ \"User\" ],\n" +
            "  \"properties\" : {\n" +
            "    \"name\" : \"foo\",\n" +
            "    \"age\" : \"42\",\n" +
            "    \"male\" : \"true\",\n" +
            "    \"kids\" : [ \"a\", \"b\", \"c\" ]\n" +
            "  },\n" +
            "  \"relationships\" : [ \"(0)-[KNOWS,0]->(1)\" ]\n" +
            "}, {\n" +
            "  \"id\" : 1,\n" +
            "  \"labels\" : [ \"User\" ],\n" +
            "  \"properties\" : {\n" +
            "    \"name\" : \"bar\",\n" +
            "    \"age\" : \"42\"\n" +
            "  },\n" +
            "  \"relationships\" : [ \"(0)-[KNOWS,0]->(1)\" ]\n" +
            "}, {\n" +
            "  \"id\" : 2,\n" +
            "  \"labels\" : [ \"User\" ],\n" +
            "  \"properties\" : {\n" +
            "    \"age\" : \"12\"\n" +
            "  },\n" +
            "  \"relationships\" : [ ]\n" +
            "} ]");

    private static final String EXPECTED_QUERY = String.format("[ {\n" +
            "  \"u.age\" : \"42\",\n" +
            "  \"u.name\" : \"foo\",\n" +
            "  \"u.male\" : \"true\",\n" +
            "  \"u.kids\" : [ \"a\", \"b\", \"c\" ],\n" +
            "  \"labels(u)\" : \"[User]\"\n" +
            "}, {\n" +
            "  \"u.age\" : \"42\",\n" +
            "  \"u.name\" : \"bar\",\n" +
            "  \"labels(u)\" : \"[User]\"\n" +
            "}, {\n" +
            "  \"u.age\" : \"12\",\n" +
            "  \"labels(u)\" : \"[User]\"\n" +
            "} ]");

    private static final String EXPECTED_COUNT = String.format("[ {\n" +
            "  \"count(n)\" : \"3\"\n" +
            "} ]");

    private static final String EXPECTED = String.format("[ {\n" +
            "  \"id\" : 0,\n" +
            "  \"labels\" : [ \"User\" ],\n" +
            "  \"properties\" : {\n" +
            "    \"name\" : \"foo\",\n" +
            "    \"age\" : \"42\",\n" +
            "    \"male\" : \"true\",\n" +
            "    \"kids\" : [ \"a\", \"b\", \"c\" ]\n" +
            "  },\n" +
            "  \"relationships\" : [ \"(0)-[KNOWS,0]->(1)\" ]\n" +
            "}, {\n" +
            "  \"id\" : 1,\n" +
            "  \"labels\" : [ \"User\" ],\n" +
            "  \"properties\" : {\n" +
            "    \"name\" : \"bar\",\n" +
            "    \"age\" : \"42\"\n" +
            "  },\n" +
            "  \"relationships\" : [ \"(0)-[KNOWS,0]->(1)\" ]\n" +
            "}, {\n" +
            "  \"id\" : 2,\n" +
            "  \"labels\" : [ \"User\" ],\n" +
            "  \"properties\" : {\n" +
            "    \"age\" : \"12\"\n" +
            "  },\n" +
            "  \"relationships\" : [ ]\n" +
            "}, {\n" +
            "  \"id\" : 0,\n" +
            "  \"type\" : \"KNOWS\",\n" +
            "  \"properties\" : { },\n" +
            "  \"start_node_id\" : 0,\n" +
            "  \"end_node_id\" : 1\n" +
            "} ]");

    private static final String EXCEPTED_QUERY_TWO_NODES = "[ {\n"+
            "  \"id\" : 0,\n"+
            "  \"labels\" : [ \"User\" ],\n"+
            "  \"properties\" : {\n"+
            "    \"name\" : \"foo\",\n"+
            "    \"age\" : \"42\",\n"+
            "    \"male\" : \"true\",\n"+
            "    \"kids\" : [ \"a\", \"b\", \"c\" ]\n"+
            "  },\n"+
            "  \"relationships\" : [ \"(0)-[KNOWS,0]->(1)\" ]\n"+
            "}, {\n"+
            "  \"id\" : 1,\n"+
            "  \"labels\" : [ \"User\" ],\n"+
            "  \"properties\" : {\n"+
            "    \"name\" : \"bar\",\n"+
            "    \"age\" : \"42\"\n"+
            "  },\n"+
            "  \"relationships\" : [ \"(0)-[KNOWS,0]->(1)\" ]\n"+
            "} ]";

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ExportJson.class, Graphs.class);
        db.execute("CREATE (f:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})").close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExportAllJson() throws Exception {
        File output = new File(directory, "all.json");
        TestUtil.testCall(db, "CALL apoc.export.json.all({file},null)", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertResults(output, r, "database");
                }
        );
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphJson() throws Exception {
        File output = new File(directory, "graph.json");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.json.graph(graph, {file},null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryJson() throws Exception {
        File output = new File(directory, "query.json");
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file},null)", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(5)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryNodesJson() throws Exception {
        File output = new File(directory, "query_nodes.json");
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file},null)", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_NODES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryTwoNodesJson() throws Exception {
        File output = new File(directory, "query_two_nodes.json");
        String query = "MATCH (u:User{name:'foo'}), (l:User{name:'bar'}) return u, l";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file},null)", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(2)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));

                });
        assertEquals(EXCEPTED_QUERY_TWO_NODES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryNodesJsonParams() throws Exception {
        File output = new File(directory, "query_nodes_param.json");
        String query = "MATCH (u:User) WHERE u.age > {age} return u";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file},{params:{age:10}})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_NODES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryNodesJsonCount() throws Exception {
        File output = new File(directory, "query_nodes_count.json");
        String query = "MATCH (n) return count(n)";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file},null)", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));

                });
        assertEquals(EXPECTED_COUNT, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportData() throws Exception {
        File output = new File(directory, "data.json");
        TestUtil.testCall(db, "MATCH (nod:User) " +
                        "MATCH ()-[reels:KNOWS]->() " +
                        "WITH collect(nod) as a, collect(reels) as b "+
                        "CALL apoc.export.json.data(a, b, {file}, null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(7L, r.get("properties"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals(output.getAbsolutePath(), r.get("file"));
        assertEquals("json", r.get("format"));
        assertEquals(true, ((long) r.get("time")) >= 0);
    }
}
