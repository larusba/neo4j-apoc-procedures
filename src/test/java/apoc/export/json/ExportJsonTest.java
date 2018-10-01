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
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;

public class ExportJsonTest {

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");
    private static Map<String,String> jsonResults;

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

        populateJsonResultsMap();

    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExportAllJson() throws Exception {
        String filename = "all.json";
        File output = new File(directory, filename);
        TestUtil.testCall(db, "CALL apoc.export.json.all({file})", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertResults(output, r, "database");
                }
        );
        assertEquals(jsonResults.get("all"), new Scanner(output).useDelimiter("\\Z").next());
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
        assertEquals(jsonResults.get("mapPointDatetime"), new Scanner(output).useDelimiter("\\Z").next());
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
        assertEquals(jsonResults.get("graph"), new Scanner(output).useDelimiter("\\Z").next());
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
        assertEquals(jsonResults.get("query"), new Scanner(output).useDelimiter("\\Z").next());
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
        assertEquals(jsonResults.get("query_nodes"), new Scanner(output).useDelimiter("\\Z").next());
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
        assertEquals(jsonResults.get("query_two_nodes"), new Scanner(output).useDelimiter("\\Z").next());
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
        assertEquals(jsonResults.get("query_nodes_param"), new Scanner(output).useDelimiter("\\Z").next());
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
        assertEquals(jsonResults.get("query_nodes_count"), new Scanner(output).useDelimiter("\\Z").next());
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
        assertEquals(jsonResults.get("data"), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportDataPath() throws Exception {
        String filename = "query_nodes_path.json";
        File output = new File(directory, filename);
        String query = "MATCH p = (u:User)-[rel]->(u2:User) return u, rel, u2, p, u.id, u.name";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(jsonResults.get("query_nodes_path"), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportAllWithDataTypesJson() throws Exception {
        String filename = "all.json";
        File output = new File(directory, filename);
        TestUtil.testCall(db, "CALL apoc.export.json.all({file},{writeDataTypes:true})", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertResults(output, r, "database");
                }
        );
        assertEquals(jsonResults.get("all_with_datatypes"), new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryWithDataTypesJson() throws Exception {
        String filename = "query.json";
        File output = new File(directory, filename);
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.json.query({query},{file},{useTypes:true})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("kernelTransaction: cols(5)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertEquals(jsonResults.get("query_with_datatypes"), new Scanner(output).useDelimiter("\\Z").next());
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

    private static void populateJsonResultsMap(){

        jsonResults = new HashMap<>();

        jsonResults.put("all", "{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]}}\n" +
                "{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42}}\n" +
                "{\"type\":\"node\",\"id\":2,\"labels\":[\"User\"],\"data\":{\"age\":12}}\n" +
                "{\"type\":\"relationship\",\"id\":0,\"label\":\"KNOWS\",\"data\":{},\"start\":0,\"end\":1}");

        jsonResults.put("data", "{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]}}\n" +
                "{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42}}\n" +
                "{\"type\":\"node\",\"id\":2,\"labels\":[\"User\"],\"data\":{\"age\":12}}\n" +
                "{\"type\":\"relationship\",\"id\":0,\"label\":\"KNOWS\",\"data\":{},\"start\":0,\"end\":1}");

        jsonResults.put("graph", "{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]}}\n" +
                "{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42}}\n" +
                "{\"type\":\"node\",\"id\":2,\"labels\":[\"User\"],\"data\":{\"age\":12}}\n" +
                "{\"type\":\"relationship\",\"id\":0,\"label\":\"KNOWS\",\"data\":{},\"start\":0,\"end\":1}");

        jsonResults.put("mapPointDatetime", "{\"data\":{\"map\":{\"a\":1,\"b\":{\"c\":1,\"d\":\"a\",\"e\":{\"f\":[1,3,5]}}}," +
                "\"theDateTime\":\"2015-06-24T12:50:35.556+01:00\"," +
                "\"theLocalDateTime\":\"2015-07-04T19:32:24\"," +
                "\"point\":{\"crs\":{\"name\":\"wgs-84\",\"table\":\"EPSG\",\"code\":4326,\"href\":\"http://spatialreference.org/ref/epsg/4326/\",\"dimension\":2,\"geographic\":true,\"calculator\":{},\"type\":\"wgs-84\"},\"coordinates\":[33.46789,13.1]}," +
                "\"date\":\"2015-03-26\",\"time\":\"12:50:35.556+01:00\",\"localTime\":\"12:50:35.556\"}}");

        jsonResults.put("query", "{\"data\":{\"u.age\":42,\"u.name\":\"foo\",\"u.male\":true,\"u.kids\":[\"a\",\"b\",\"c\"],\"labels(u)\":[\"User\"]}}\n" +
                "{\"data\":{\"u.age\":42,\"u.name\":\"bar\",\"u.male\":null,\"u.kids\":null,\"labels(u)\":[\"User\"]}}\n" +
                "{\"data\":{\"u.age\":12,\"u.name\":null,\"u.male\":null,\"u.kids\":null,\"labels(u)\":[\"User\"]}}");

        jsonResults.put("query_nodes", "{\"u\":{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]}}}\n" +
                "{\"u\":{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42}}}\n" +
                "{\"u\":{\"type\":\"node\",\"id\":2,\"labels\":[\"User\"],\"data\":{\"age\":12}}}");

        jsonResults.put("query_nodes_count", "{\"data\":{\"count(n)\":3}}");

        jsonResults.put("query_nodes_param", "{\"u\":{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]}}}\n" +
                "{\"u\":{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42}}}\n" +
                "{\"u\":{\"type\":\"node\",\"id\":2,\"labels\":[\"User\"],\"data\":{\"age\":12}}}");

        jsonResults.put("query_nodes_path", "{\"u\":{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]}}," +
                "\"rel\":{\"type\":\"relationship\",\"id\":0,\"label\":\"KNOWS\",\"data\":{},\"start\":0,\"end\":1}," +
                "\"u2\":{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42}}," +
                "\"p\":{\"startNode\":{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]}}," +
                "\"relationship\":{\"type\":\"relationship\",\"id\":0,\"label\":\"KNOWS\",\"data\":{},\"start\":0,\"end\":1},\"endNode\":{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42}}},\"u.id\":null,\"u.name\":\"foo\"}");

        jsonResults.put("query_two_nodes", "{\"u\":{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]}}," +
                "\"l\":{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42}}}");

        jsonResults.put("all_with_datatypes", "{\"type\":\"node\",\"id\":0,\"labels\":[\"User\"],\"data\":{\"name\":\"foo\",\"age\":42,\"male\":true,\"kids\":[\"a\",\"b\",\"c\"]},\"data_types\":{\"name\":\"String\",\"age\":\"Long\",\"male\":\"Boolean\",\"kids\":\"String[]\"}}\n" +
                "{\"type\":\"node\",\"id\":1,\"labels\":[\"User\"],\"data\":{\"name\":\"bar\",\"age\":42},\"data_types\":{\"name\":\"String\",\"age\":\"Long\"}}\n" +
                "{\"type\":\"node\",\"id\":2,\"labels\":[\"User\"],\"data\":{\"age\":12},\"data_types\":{\"age\":\"Long\"}}\n" +
                "{\"type\":\"relationship\",\"id\":0,\"label\":\"KNOWS\",\"data\":{},\"data_types\":{},\"start\":0,\"end\":1}");

        jsonResults.put("query_with_datatypes", "{\"data\":{\"u.age\":42,\"data_types\":{\"u.age\":\"long\"},\"u.name\":\"foo\",\"data_types\":{\"u.name\":\"string\"},\"u.male\":true,\"data_types\":{\"u.male\":\"boolean\"},\"u.kids\":[\"a\",\"b\",\"c\"],\"data_types\":{\"u.kids\":\"string[]\"},\"labels(u)\":[\"User\"],\"data_types\":{\"labels(u)\":\"arraylist\"}}}\n" +
                "{\"data\":{\"u.age\":42,\"data_types\":{\"u.age\":\"long\"},\"u.name\":\"bar\",\"data_types\":{\"u.name\":\"string\"},\"u.male\":null,\"u.kids\":null,\"labels(u)\":[\"User\"],\"data_types\":{\"labels(u)\":\"arraylist\"}}}\n" +
                "{\"data\":{\"u.age\":12,\"data_types\":{\"u.age\":\"long\"},\"u.name\":null,\"u.male\":null,\"u.kids\":null,\"labels(u)\":[\"User\"],\"data_types\":{\"labels(u)\":\"arraylist\"}}}");
    }
}
