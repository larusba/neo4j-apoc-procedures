package apoc.bolt;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class BoltTest {

    private static final String BOLT_URL = "'bolt://neo4j:test@localhost:7687'";
    protected static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db, Bolt.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    //DATASET
    /*
    CREATE (p:Person {name:'Michael',surname:'Jordan',age:54, state:true})
    CREATE (q:Person {name:'Tom',surname:'Burton',age:23})
    CREATE (p:Person {name:'John',surname:'William',age:22})
    CREATE (q)-[:KNOWS{since:2016}]->(p)
    */

    @Test
    public void testLoadNodeVirtual() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match(p:Person {name:{name}}) return p', {name:'Michael'}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Node node = (Node) row.get("p");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Michael", node.getProperty("name"));
                assertEquals("Jordan", node.getProperty("surname"));
                assertEquals(true, node.getProperty("state"));
                assertEquals(54L, node.getProperty("age"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadNodesVirtual() throws Exception {
        TestUtil.ignoreException(() -> {
            testResult(db, "call apoc.bolt.load(" + BOLT_URL + ",'match(n) return n', {}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = r.next();
                Map result = (Map) row.get("row");
                Node node = (Node) result.get("n");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Tom", node.getProperty("name"));
                assertEquals("Burton", node.getProperty("surname"));
                assertEquals(23L, node.getProperty("age"));
                row = r.next();
                result = (Map) row.get("row");
                node = (Node) result.get("n");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("John", node.getProperty("name"));
                assertEquals("William", node.getProperty("surname"));
                assertEquals(22L, node.getProperty("age"));
                row = r.next();
                result = (Map) row.get("row");
                node = (Node) result.get("n");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Michael", node.getProperty("name"));
                assertEquals("Jordan", node.getProperty("surname"));
                assertEquals(true, node.getProperty("state"));
                assertEquals(54L, node.getProperty("age"));

            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadPathVirtual() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.bolt.load(" + BOLT_URL
                    + ",'START neo=node({idNode})  MATCH path= (neo)-[r:KNOWS*..4]->(other) return path', {idNode:1}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> path = (Map<String, Object>) row.get("path");
                Node start = (Node) path.get("start");
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("Tom", start.getProperty("name"));
                assertEquals("Burton", start.getProperty("surname"));
                assertEquals(23L, start.getProperty("age"));
                Node end = (Node) path.get("end");
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("John", end.getProperty("name"));
                assertEquals("William", end.getProperty("surname"));
                assertEquals(22L, end.getProperty("age"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadRel() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match(p)-[r]->(c) return r', {}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Relationship rel = (Relationship) row.get("r");
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(2016L, rel.getProperty("since"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadRelsAndNodes() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match(p)-[r]->(c) return *', {}, {virtual:true})", r -> {
                Map result = (Map) r.get("row");
                Node node = (Node) result.get("p");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Tom", node.getProperty("name"));
                assertEquals("Burton", node.getProperty("surname"));
                assertEquals(23L, node.getProperty("age"));
                node = (Node) result.get("c");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("John", node.getProperty("name"));
                assertEquals("William", node.getProperty("surname"));
                assertEquals(22L, node.getProperty("age"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadNullParams() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.bolt.load(\"bolt://neo4j:test@localhost:7687\",\"match(p:Person {name:'Michael'}) return p\")", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> node = (Map<String, Object>) row.get("p");
                assertTrue(node.containsKey("entityType"));
                assertEquals("NODE", node.get("entityType"));
                assertTrue(node.containsKey("properties"));
                Map<String, Object> properties = (Map<String, Object>) node.get("properties");
                assertEquals("Michael", properties.get("name"));
                assertEquals("Jordan", properties.get("surname"));
                assertEquals(54L, properties.get("age"));
                assertEquals(true, properties.get("state"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadNode() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match (p:Person {name:{name}}) return p', {name:'Michael'})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> node = (Map<String, Object>) row.get("p");
                assertTrue(node.containsKey("entityType"));
                assertEquals("NODE", node.get("entityType"));
                assertTrue(node.containsKey("properties"));
                Map<String, Object> properties = (Map<String, Object>) node.get("properties");
                assertEquals("Michael", properties.get("name"));
                assertEquals("Jordan", properties.get("surname"));
                assertEquals(54L, properties.get("age"));
                assertEquals(true, properties.get("state"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadScalarSingleReusult() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match (n:Person {name:{name}}) return n.age as Age', {name:'Michael'})", (r) -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                assertTrue(row.containsKey("Age"));
                assertEquals(54L, row.get("Age"));

            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadMixedContent() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match (n:Person {name:{name}}) return n.age, n.name, n.state', {name:'Michael'})",
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = (Map<String, Object>) r.get("row");
                        assertTrue(row.containsKey("n.age"));
                        assertEquals(54L, row.get("n.age"));
                        assertTrue(row.containsKey("n.name"));
                        assertEquals("Michael", row.get("n.name"));
                        assertTrue(row.containsKey("n.state"));
                        assertEquals(true, row.get("n.state"));
                    });
        }, ConnectException.class);
    }

    @Test
    public void testLoadList() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL
                    + ",'match (p:Person {name:{name}})  with collect({personName:p.name}) as rows return rows', {name:'Michael'})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Collections> p = (List<Collections>) row.get("rows");
                Map<String, Object> result = (Map<String, Object>) p.get(0);
                assertTrue(result.containsKey("personName"));
                assertEquals("Michael", result.get("personName"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadMap() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL
                    + ",'match (p:Person {name:{name}})  with p,collect({personName:p.name}) as rows return p{.*, rows:rows}', {name:'Michael'})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> p = (Map<String, Object>) row.get("p");
                assertTrue(p.containsKey("name"));
                assertEquals("Michael", p.get("name"));
                assertTrue(p.containsKey("age"));
                assertEquals(54L, p.get("age"));
                assertTrue(p.containsKey("surname"));
                assertEquals("Jordan", p.get("surname"));
                assertTrue(p.containsKey("state"));
                assertEquals(true, p.get("state"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadPath() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'START neo=node({idNode})  MATCH path= (neo)-[r:KNOWS*..4]->(other) return path', {idNode:1})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> path = (Map<String, Object>) row.get("path");

                assertEquals("KNOWS", path.get("type"));
                assertEquals(Arrays.asList("Person"), path.get("startLabels"));
                assertEquals(Arrays.asList("Person"), path.get("endLabels"));

                Map<String, Object> startProperties = (Map<String, Object>) path.get("startProperties");
                assertEquals("Tom", startProperties.get("name"));
                assertEquals("Burton", startProperties.get("surname"));
                assertEquals(23L, startProperties.get("age"));

                Map<String, Object> endProperties = (Map<String, Object>) path.get("endProperties");
                assertEquals("John", endProperties.get("name"));
                assertEquals("William", endProperties.get("surname"));
                assertEquals(22L, endProperties.get("age"));

                Map<String, Object> relProperties = (Map<String, Object>) path.get("relProperties");
                assertEquals(2016L, relProperties.get("since"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadRels() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match (n)-[r]->(c) return r as rel', {})", (r) -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> rel = (Map<String, Object>) row.get("rel");
                assertEquals(1L, rel.get("start"));
                assertEquals(8L, rel.get("end"));
                assertEquals("RELATIONSHIP", rel.get("entityType"));
                assertEquals("KNOWS", rel.get("type"));
                Map<String, Object> properties = (Map<String, Object>) rel.get("properties");
                assertEquals(2016L, properties.get("since"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testLoadCreateNodeStatistic() throws Exception {
        TestUtil.ignoreException(() -> {
            testResult(db, "call apoc.bolt.execute(" + BOLT_URL + ",'create(n:Node {name:{name}})', {name:'Node1'}, {statistics:true})", Collections.emptyMap(),
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = r.next();
                        Map result = (Map) row.get("row");
                        assertEquals(1L, toLong(result.get("nodesCreated")));
                        assertEquals(1L, toLong(result.get("labelsAdded")));
                        assertEquals(1L, toLong(result.get("propertiesSet")));
                        assertEquals(false, r.hasNext());
                    });
        }, ConnectException.class);
    }

    @Test
    public void testLoadNoVirtual() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db,
                    "call apoc.bolt.load(\"bolt://neo4j:test@localhost:7687\",\"match(p:Person {name:'Michael'}) return p\", {}, {virtual:false, test:false})",
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = (Map<String, Object>) r.get("row");
                        Map<String, Object> node = (Map<String, Object>) row.get("p");
                        assertTrue(node.containsKey("entityType"));
                        assertEquals("NODE", node.get("entityType"));
                        assertTrue(node.containsKey("properties"));
                        Map<String, Object> properties = (Map<String, Object>) node.get("properties");
                        assertEquals("Michael", properties.get("name"));
                        assertEquals("Jordan", properties.get("surname"));
                        assertEquals(54L, properties.get("age"));
                        assertEquals(true, properties.get("state"));
                    });
        }, ConnectException.class);
    }

    private long toLong(Object value) {
        return Util.toLong(value);
    }
}

