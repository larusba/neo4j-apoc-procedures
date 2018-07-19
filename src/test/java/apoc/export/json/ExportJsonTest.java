package apoc.export.json;

import apoc.export.util.JsonResults;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;

public class ExportJsonTest {

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
        String filename = "all.json";
        File output = new File(directory, filename);
        TestUtil.testCall(db, "CALL apoc.export.json.all({file},null)", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertResults(output, r, "database");
                }
        );
        assertEquals(JsonResults.expectedAllGraphData(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportPointMapDatetimeJson() throws Exception {
        String filename = "mapPointDatetime.json";
        File output = new File(directory, filename);
        //String query = "return time() as date";
        String query = "return {a: 1, b: {c: 1, d:'a', e: {f: [1,3,5]}}} as map, " +
                "datetime('2015-06-24T12:50:35.556+0100') AS theDateTime, " +
                "localdatetime('2015185T19:32:24') AS theLocalDateTime," +
                "point({latitude: 13.1, longitude: 33.46789}) as point," +
                "date('+2015-W13-4') as date," +
                "time('125035.556+0100') as time," +
                "localTime('12:50:35.556') as localTime";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(7)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(JsonResults.expectedMapPointDatetime(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphJson() throws Exception {
        String filename = "graph.json";
        File output = new File(directory, filename);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.json.graph(graph, {file}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(JsonResults.expectedAllGraphData(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryJson() throws Exception {
        String filename = "query.json";
        File output = new File(directory, filename);
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(5)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(JsonResults.expectedQueryData(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryNodesJson() throws Exception {
        String filename = "query_nodes.json";
        File output = new File(directory, filename);
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(JsonResults.expectedQueryNodes(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryTwoNodesJson() throws Exception {
        String filename = "query_two_nodes.json";
        File output = new File(directory, filename);
        String query = "MATCH (u:User{name:'foo'}), (l:User{name:'bar'}) return u, l";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(2)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(JsonResults.expectedQueryTwoNodes(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryNodesJsonParams() throws Exception {
        String filename = "query_nodes_param.json";
        File output = new File(directory, filename);
        String query = "MATCH (u:User) WHERE u.age > {age} return u";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file},{params:{age:10}})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(JsonResults.expectedQueryNodes(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryNodesJsonCount() throws Exception {
        String filename = "query_nodes_count.json";
        File output = new File(directory, filename);
        String query = "MATCH (n) return count(n)";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(JsonResults.expectedQueryNodesCount(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportData() throws Exception {
        String filename = "data.json";
        File output = new File(directory, filename);
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
        assertEquals(JsonResults.expectedAllGraphData(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportDataPath() throws Exception {
        String filename = "query_nodes_path.json";
        File output = new File(directory, filename);
        String query = "MATCH p = (u:User)-[rel]->(u2:User) return u, rel, u2, p, u.name";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(JsonResults.expectedQueryPath(), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportAllWithDataTypesJson() throws Exception {
        String filename = "all.json";
        File output = new File(directory, filename);
        TestUtil.testCall(db, "CALL apoc.export.json.all({file},{useTypes:true})", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertResults(output, r, "database");
                }
        );
        assertEquals(JsonResults.expectedAllWithDataTypes(), new Scanner(output).useDelimiter("\\Z").next());
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
