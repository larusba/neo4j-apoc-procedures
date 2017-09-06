package apoc.execute;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.*;

import static apoc.util.TestUtil.testResult;

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
    public void testExecuteNode() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"match (p:Person {name:'Michael'}) return p\", {}, {})", r -> {
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
    public void testExecuteCreateNodeStatistic() throws Exception {
        TestUtil.ignoreException(() -> {
            testResult(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"create(n:Node {name:'Node1'})\", {}, {statistics:true})", Collections.emptyMap(), r -> {
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

    @Ignore
    @Test
    public void testExecuteScalar() throws Exception {
        TestUtil.ignoreException(() -> {
            testResult(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"match (n:Person {name:'Michael'}) return n.age as Age\", {}, {})", Collections.emptyMap(), (r) -> {
                assertNotNull(r.hasNext());
                Map<String, Object> row = (Map) r.next().get("row");
                assertTrue(row.containsKey("Age"));
                assertEquals(54L, row.get("Age"));
                assertNotNull(r.hasNext());
                row = (Map) r.next().get("row");
                assertTrue(row.containsKey("Age"));
                assertEquals(25L, ((Long) row.get("Age")).longValue());

            });
        }, ConnectException.class);
    }

    @Test
    public void testExecuteScalarSingleReusult() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"match (n:Person {name:'Michael'}) return n.age as Age\", {}, {})", (r) -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                assertTrue(row.containsKey("Age"));
                assertEquals(54L, row.get("Age"));

            });
        }, ConnectException.class);
    }

    @Test
    public void testExecuteMixedContent() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"match (n:Person {name:'Michael'}) return n.age, n.name, n.state\", {}, {})", r -> {
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
    public void testExecuteList() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"match (p:Person {name:'Michael'})  with collect({personName:p.name}) as rows return rows\", {}, {})", r -> {
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
    public void testExecuteMap() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"match (p:Person {name:'Michael'})  with p,collect({personName:p.name}) as rows return p{.*, rows:rows}\", {}, {})", r -> {
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
    public void testExecutePath() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"START neo=node(1)  MATCH path= (neo)-[r:KNOWS*..4]->(other) return path\", {}, {})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                assertEquals("KNOWS", row.get("type"));
                assertEquals(Arrays.asList("Person"), row.get("startLabels"));
                assertEquals(Arrays.asList("Person"), row.get("endLabels"));

                Map<String, Object> startProperties = (Map<String, Object>) row.get("startProperties");
                assertEquals("Tom", startProperties.get("name"));
                assertEquals("Burton", startProperties.get("surname"));
                assertEquals(23L, startProperties.get("age"));

                Map<String, Object> endProperties = (Map<String, Object>) row.get("endProperties");
                assertEquals("John", endProperties.get("name"));
                assertEquals("William", endProperties.get("surname"));
                assertEquals(22L, endProperties.get("age"));

                Map<String, Object> relProperties = (Map<String, Object>) row.get("relProperties");
                assertEquals(2016L, relProperties.get("since"));
            });
        }, ConnectException.class);
    }

    @Test
    public void testExecuteRels() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "call apoc.execute.bolt(\"bolt://neo4j:test@localhost:7687\",\"match (n)-[r]->(c) return r as rel\", {}, {})", (r) -> {
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

    private long toLong(Object value) {
        return Util.toLong(value);
    }
}
