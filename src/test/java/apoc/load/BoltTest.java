package apoc.load;

import apoc.load.Bolt;
import apoc.result.VirtualNode;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class BoltTest {

    protected static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
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
    public void testLoadNode() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.load.bolt(\"bolt://neo4j:test@localhost:7687\",\"match(p:Person {name:'Michael'}) return p\", {}, {virtual:true})", r -> {
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
    public void testLoadNodes() throws Exception {
        TestUtil.ignoreException(() -> {
            testResult(db, "call apoc.load.bolt(\"bolt://neo4j:test@localhost:7687\",\"match(n) return n\", {}, {virtual:true})", r -> {
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
    public void testLoadPath() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.load.bolt(\"bolt://neo4j:test@localhost:7687\",\"START neo=node(177)  MATCH path= (neo)-[r:KNOWS*..4]->(other) return path\", {}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Node start = (Node) row.get("start");
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("Tom", start.getProperty("name"));
                assertEquals("Burton", start.getProperty("surname"));
                assertEquals(23L, start.getProperty("age"));
                Node end = (Node) row.get("end");
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
            testCall(db, "call apoc.load.bolt(\"bolt://neo4j:test@localhost:7687\",\"match(p)-[r]->(c) return r\", {}, {virtual:true})", r -> {
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
            testCall(db, "call apoc.load.bolt(\"bolt://neo4j:test@localhost:7687\",\"match(p)-[r]->(c) return *\", {}, {virtual:true})", r -> {
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
}
