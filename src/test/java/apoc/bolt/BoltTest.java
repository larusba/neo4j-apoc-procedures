package apoc.bolt;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.*;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.graphdb.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.values.storable.DurationValue;

import java.time.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.neo4j.driver.v1.Values.isoDuration;
import static org.neo4j.driver.v1.Values.point;
import static org.neo4j.values.storable.CoordinateReferenceSystem.*;
import static org.neo4j.values.storable.Values.pointValue;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class BoltTest {

    protected static GraphDatabaseService db;

    private static String setup = "CREATE (m:Person {name:'Michael',surname:'Jordan',age:54, state:true, date:date('2018-10-30')})\n" +
            "    CREATE (q:Person {name:'Tom',surname:'Burton',age:23, date:datetime('2018-10-30T12:50:35.556+0100')})\n" +
            "    CREATE (p:Person {name:'John',surname:'William',age:22, date:localdatetime('20181030T19:32:24')})\n" +
            "    CREATE (q)-[:KNOWS{since:2016, time:time('125035.556+0100')}]->(p)\n" +
            "    CREATE (a:Person{name:'Tom', surname:'Loagan', duration:duration('P5M1DT12H')})\n" +
            "    CREATE (b:Person{name:'John', surname:'Green', born:point({ x: 2.3, y: 4.5 })})\n" +
            "    CREATE (c:Person{name:'Jim', surname:'Brown'})\n" +
            "    CREATE (d:Person{name:'Anne', surname:'Olsson', born:point({ x: 2.3, y: 4.5 , z: 1.2})})\n" +
            "    CREATE (a)-[:KNOWS{since:localtime('12:50:35.556'), born:point({ longitude: 56.7, latitude: 12.78 })}]->(b)\n" +
            "    CREATE (b)-[:KNOWS{since:time('125035.556+0100'), born:point({ longitude: 56.7, latitude: 12.78, height: 100 })}]->(c)\n" +
            "    CREATE (c)-[:KNOWS{since:2013}]->(d)";

    public static ServerControls server;
    private static String BOLT_URL;

    @BeforeClass
    public static void setUp() throws Exception {
        server = TestServerBuilders.newInProcessBuilder().withFixture(setup)
                .withConfig("dbms.security.auth_enabled", "false").newServer();
        BOLT_URL = "'"+server.boltURI().toString()+"'";

        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db, Bolt.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
        server.close();
    }

    @Test
    public void testLoadNodeVirtual() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match(p:Person {name:{name}}) return p', {name:'Michael'}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Node node = (Node) row.get("p");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Michael", node.getProperty("name"));
                assertEquals("Jordan", node.getProperty("surname"));
                assertEquals(true, node.getProperty("state"));
                assertEquals(54L, node.getProperty("age"));
            });
    }

    @Test
    public void testLoadNodesVirtual() throws Exception {
            TestUtil.testResult(db, "call apoc.bolt.load(" + BOLT_URL + ",'match(n) return n', {}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = r.next();
                Map result = (Map) row.get("row");
                Node node = (Node) result.get("n");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Michael", node.getProperty("name"));
                assertEquals("Jordan", node.getProperty("surname"));
                assertEquals(true, node.getProperty("state"));
                assertEquals(54L, node.getProperty("age"));
                row = r.next();
                result = (Map) row.get("row");
                node = (Node) result.get("n");
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
            });
    }

    @Test
    public void testLoadPathVirtual() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'START neo=node({idNode})  MATCH path= (neo)-[r:KNOWS*..3]->(other) return path', {idNode:1}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Object> path = (List<Object>) row.get("path");
                Node start = (Node) path.get(0);
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("Tom", start.getProperty("name"));
                assertEquals("Burton", start.getProperty("surname"));
                Node end = (Node) path.get(2);
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("John", end.getProperty("name"));
                assertEquals("William", end.getProperty("surname"));
                Relationship rel = (Relationship) path.get(1);
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(2016L, rel.getProperty("since"));
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), rel.getProperty("time"));
            });
    }

    @Test
    public void testLoadRel() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match(p)-[r]->(c) return r limit 1', {}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Relationship rel = (Relationship) row.get("r");
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(2016L, rel.getProperty("since"));
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), rel.getProperty("time"));
            });
    }

    @Test
    public void testLoadRelsAndNodes() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match(p:Person {surname:{surnameP}})-[r]->(c:Person {surname:{surnameC}}) return *', {surnameP:\"Burton\", surnameC:\"William\"}, {virtual:true})", r -> {
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
    }

    @Test
    public void testLoadNullParams() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load("+BOLT_URL+",\"match(p:Person {name:'Michael'}) return p\")", r -> {
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
    }

    @Test
    public void testLoadNode() throws Exception {
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
    }

    @Test
    public void testLoadScalarSingleReusult() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match (n:Person {name:{name}}) return n.age as Age', {name:'Michael'})", (r) -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                assertTrue(row.containsKey("Age"));
                assertEquals(54L, row.get("Age"));
            });
    }

    @Test
    public void testLoadMixedContent() throws Exception {
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
    }

    @Test
    public void testLoadList() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match (p:Person {name:{name}})  with collect({personName:p.name}) as rows return rows', {name:'Michael'})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Collections> p = (List<Collections>) row.get("rows");
                Map<String, Object> result = (Map<String, Object>) p.get(0);
                assertTrue(result.containsKey("personName"));
                assertEquals("Michael", result.get("personName"));
            });
    }

    @Test
    public void testLoadMap() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match (p:Person {name:{name}})  with p,collect({personName:p.name}) as rows return p{.*, rows:rows}', {name:'Michael'})", r -> {
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
    }

    @Test
    public void testLoadPath() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'MATCH path= (neo)-[r:KNOWS*..3]->(other) where id(neo) = {idNode} return path', {idNode:1}, {})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Object> path = (List<Object>) row.get("path");
                Map<String, Object> startNode = (Map<String, Object>) path.get(0);
                assertEquals("NODE", startNode.get("entityType"));
                assertEquals(Arrays.asList("Person"), startNode.get("labels"));
                Map<String, Object> startNodeProperties = (Map<String, Object>) startNode.get("properties");
                assertEquals("Tom", startNodeProperties.get("name"));
                assertEquals("Burton", startNodeProperties.get("surname"));
                Map<String, Object> endNode = (Map<String, Object>) path.get(2);
                assertEquals("NODE", startNode.get("entityType"));
                assertEquals(Arrays.asList("Person"), startNode.get("labels"));
                Map<String, Object> endNodeProperties = (Map<String, Object>) endNode.get("properties");
                assertEquals("John", endNodeProperties.get("name"));
                assertEquals("William", endNodeProperties.get("surname"));
                Map<String, Object> rel = (Map<String, Object>) path.get(1);
                assertEquals("RELATIONSHIP", rel.get("entityType"));
                assertEquals("KNOWS", rel.get("type"));
                Map<String, Object> relProperties = (Map<String, Object>) rel.get("properties");
                assertEquals(2016L, relProperties.get("since"));
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), relProperties.get("time"));
            });
    }

    @Test
    public void testLoadRels() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'match (n)-[r]->(c) return r as rel limit 1', {})", (r) -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> rel = (Map<String, Object>) row.get("rel");
                assertEquals(1L, rel.get("start"));
                assertEquals(2L, rel.get("end"));
                assertEquals("RELATIONSHIP", rel.get("entityType"));
                assertEquals("KNOWS", rel.get("type"));
                Map<String, Object> properties = (Map<String, Object>) rel.get("properties");
                assertEquals(2016L, properties.get("since"));
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), properties.get("time"));
            });
    }

    @Test
    public void testExecuteCreateNodeStatistic() throws Exception {
            TestUtil.testResult(db, "call apoc.bolt.execute(" + BOLT_URL + ",'create(n:Node {name:{name}})', {name:'Node1'}, {statistics:true})", Collections.emptyMap(),
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = r.next();
                        Map result = (Map) row.get("row");
                        assertEquals(1L, (long) Util.toLong(result.get("nodesCreated")));
                        assertEquals(1L, (long) Util.toLong(result.get("labelsAdded")));
                        assertEquals(1L, (long) Util.toLong(result.get("propertiesSet")));
                        assertEquals(false, r.hasNext());
                    });
    }

    @Test
    public void testExecuteCreateVirtualNode() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.execute(" + BOLT_URL + ",'create(n:Node {name:{name}}) return n', {name:'Node1'}, {virtual:true})",
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = (Map<String, Object>) r.get("row");
                        Node node = (Node) row.get("n");
                        assertEquals(true, node.hasLabel(Label.label("Node")));
                        assertEquals("Node1", node.getProperty("name"));
                    });
    }

    @Test
    public void testLoadNoVirtual() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load("+BOLT_URL+",\"match(p:Person {name:'Michael'}) return p\", {}, {virtual:false, test:false})",
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
    }

    @Test
    public void testLoadNodeWithDriverConfig() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",\"match(p:Person {name:{nameP}}) return p\", {nameP:'Michael'}, " +
                            "{driverConfig:{logging:'WARNING', encryption: false,logLeakedSessions:true, maxIdleConnectionPoolSize:10, idleTimeBeforeConnectionTest:-1," +
                            " routingFailureLimit: 1, routingRetryDelayMillis:500, connectionTimeoutMillis:500, maxRetryTimeMs:30000 , trustStrategy:'TRUST_ALL_CERTIFICATES'}})",
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
    }

    @Test
    public void testLoadBigPathVirtual() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + BOLT_URL + ",'MATCH path= (neo)-[r:KNOWS*3]->(other) WHERE id(neo) = {idNode} return path', {idNode:3}, {virtual:true})", r -> {
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Object> path = (List<Object>) row.get("path");
                Node start = (Node) path.get(0);
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("Tom", start.getProperty("name"));
                assertEquals("Loagan", start.getProperty("surname"));
                assertEquals(isoDuration(5, 1, 43200, 0).asIsoDuration(), start.getProperty("duration"));
                Relationship rel = (Relationship) path.get(1);
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(LocalTime.parse("12:50:35.556"), rel.getProperty("since"));
                assertEquals(point(4326, 56.7, 12.78).asPoint(), rel.getProperty("born"));
                Node end = (Node) path.get(2);
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("John", end.getProperty("name"));
                assertEquals("Green", end.getProperty("surname"));
                assertEquals(point(7203, 2.3, 4.5).asPoint(), end.getProperty("born"));
                start = (Node) path.get(3);
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("John", start.getProperty("name"));
                assertEquals("Green", start.getProperty("surname"));
                assertEquals(point(7203, 2.3, 4.5).asPoint(), end.getProperty("born"));
                rel = (Relationship) path.get(4);
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), rel.getProperty("since"));
                assertEquals(point(4979, 56.7, 12.78, 100.0).asPoint(), rel.getProperty("born"));
                end = (Node) path.get(5);
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("Jim", end.getProperty("name"));
                assertEquals("Brown", end.getProperty("surname"));
                start = (Node) path.get(6);
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("Jim", start.getProperty("name"));
                assertEquals("Brown", start.getProperty("surname"));
                rel = (Relationship) path.get(7);
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(2013L, rel.getProperty("since"));
                end = (Node) path.get(8);
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("Anne", end.getProperty("name"));
                assertEquals("Olsson", end.getProperty("surname"));
                assertEquals(point(9157,2.3, 4.5, 1.2).asPoint(), end.getProperty("born"));
            });
    }

}

