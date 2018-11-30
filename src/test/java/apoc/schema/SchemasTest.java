package apoc.schema;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestUtil.*;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author mh
 * @since 12.05.16
 */
public class SchemasTest {
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = apocEnterpriseGraphDatabaseBuilder().newGraphDatabase();
        registerProcedure(db, Schemas.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testCreateIndex() throws Exception {
        testCall(db, "CALL apoc.schema.assert({Foo:['bar']},null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes(Label.label("Foo")));
            assertEquals(1, indexes.size());
            assertEquals("Foo", indexes.get(0).getLabel().name());
            assertEquals(asList("bar"), indexes.get(0).getPropertyKeys());
        }
    }

    @Test
    public void testCreateSchema() throws Exception {
        testCall(db, "CALL apoc.schema.assert(null,{Foo:['bar']})", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints(Label.label("Foo")));
            assertEquals(1, constraints.size());
            ConstraintDefinition constraint = constraints.get(0);
            assertEquals(ConstraintType.UNIQUENESS, constraint.getConstraintType());
            assertEquals("Foo", constraint.getLabel().name());
            assertEquals("bar", Iterables.single(constraint.getPropertyKeys()));
        }
    }

    @Test
    public void testDropIndexWhenUsingDropExisting() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testCall(db, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(0, indexes.size());
        }
    }

    @Test
    public void testDropIndexAndCreateIndexWhenUsingDropExisting() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testResult(db, "CALL apoc.schema.assert({Bar:['foo']},null)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testRetainIndexWhenNotUsingDropExisting() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testResult(db, "CALL apoc.schema.assert({Bar:['foo', 'bar']}, null, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(3, indexes.size());
        }
    }

    @Test
    public void testDropSchemaWhenUsingDropExisting() throws Exception {
        db.execute("CREATE CONSTRAINT ON (f:Foo) ASSERT f.bar IS UNIQUE").close();
        testCall(db, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(0, constraints.size());
        }
    }

    @Test
    public void testDropSchemaAndCreateSchemaWhenUsingDropExisting() throws Exception {
        db.execute("CREATE CONSTRAINT ON (f:Foo) ASSERT f.bar IS UNIQUE").close();
        testResult(db, "CALL apoc.schema.assert(null, {Bar:['foo']})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(1, constraints.size());
        }
    }

    @Test
    public void testRetainSchemaWhenNotUsingDropExisting() throws Exception {
        db.execute("CREATE CONSTRAINT ON (f:Foo) ASSERT f.bar IS UNIQUE").close();
        testResult(db, "CALL apoc.schema.assert(null, {Bar:['foo', 'bar']}, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(3, constraints.size());
        }
    }

    @Test
    public void testKeepIndex() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testResult(db, "CALL apoc.schema.assert({Foo:['bar', 'foo']},null,false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(2, indexes.size());
        }
    }

    @Test
    public void testKeepSchema() throws Exception {
        db.execute("CREATE CONSTRAINT ON (f:Foo) ASSERT f.bar IS UNIQUE").close();
        testResult(db, "CALL apoc.schema.assert(null,{Foo:['bar', 'foo']})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(expectedKeys("bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(2, constraints.size());
        }
    }

    @Test
    public void testIndexes() {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        try (Transaction tx = db.beginTx()) {
            db.schema().awaitIndexesOnline(5, TimeUnit.SECONDS);
            tx.success();
        }
        testCall(db, "CALL apoc.schema.nodes()", (result) -> {
            // Get the index info

            assertEquals(":Foo(bar)", result.get("name"));
            assertEquals("ONLINE", result.get("status"));
            assertEquals("Foo", result.get("label"));
            assertEquals("INDEX", result.get("type"));
            assertEquals("bar", ((List<String>) result.get("properties")).get(0));
            assertEquals("NO FAILURE", result.get("failure"));
            assertEquals(new Double(100), result.get("populationProgress"));
            assertEquals(new Double(1), result.get("valuesSelectivity"));
            assertEquals("Index( GENERAL, :Foo(bar) )", result.get("userDescription"));
        });
    }

    @Test
    public void testIndexesByLabel() {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        db.execute("CREATE INDEX ON :Bar(foo)").close();
        try (Transaction tx = db.beginTx()) {
            db.schema().awaitIndexesOnline(5, TimeUnit.SECONDS);
            tx.success();
        }
        testCall(db, "CALL apoc.schema.nodes('Foo')", (result) -> {
            // Get the index info
            assertEquals(":Foo(bar)", result.get("name"));
            assertEquals("ONLINE", result.get("status"));
            assertEquals("Foo", result.get("label"));
            assertEquals("INDEX", result.get("type"));
            assertEquals("bar", ((List<String>) result.get("properties")).get(0));
            assertEquals("NO FAILURE", result.get("failure"));
            assertEquals(new Double(100), result.get("populationProgress"));
            assertEquals(new Double(1), result.get("valuesSelectivity"));
            assertEquals("Index( GENERAL, :Foo(bar) )", result.get("userDescription"));

        });
    }

    @Test(expected = RuntimeException.class)
    public void testIndexesByLabelWithError() {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        try (Transaction tx = db.beginTx()) {
            db.schema().awaitIndexesOnline(5, TimeUnit.SECONDS);
            tx.success();
        }
        try {
            testCall(db, "CALL apoc.schema.nodes('Wrong')", (result) -> {});
        } catch (RuntimeException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("Label Wrong not found!", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testIndexExists() {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testResult(db, "RETURN apoc.schema.node.indexExists('Foo', ['bar'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(true, r.entrySet().iterator().next().getValue());
        });
    }

    @Test
    public void testIndexNotExists() {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testResult(db, "RETURN apoc.schema.node.indexExists('Bar', ['foo'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(false, r.entrySet().iterator().next().getValue());
        });
    }

    /**
     * This is only for version 3.2
     */
    @Test
    public void testIndexOnMultipleProperties() {
        db.execute("CREATE INDEX ON :Foo(bar, foo)").close();
        db.execute("CALL db.awaitIndex(':Foo(bar, foo)')").close();
        testCall(db, "CALL apoc.schema.nodes()", (result) -> {
            // Get the index info
            assertEquals(":Foo(bar,foo)", result.get("name"));
            assertEquals("ONLINE", result.get("status"));
            assertEquals("Foo", result.get("label"));
            assertEquals("INDEX", result.get("type"));
            assertTrue(((List<String>) result.get("properties")).contains("bar"));
            assertTrue(((List<String>) result.get("properties")).contains("foo"));
        });
    }

    @Test
    public void testUniquenessConstraintOnNode() {
        db.execute("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.foo IS UNIQUE").close();
        testCall(db, "CALL apoc.schema.nodes()", (result) -> {
            assertEquals(":Bar(foo)", result.get("name"));
            assertEquals("Bar", result.get("label"));
            assertEquals("UNIQUENESS", result.get("type"));
            assertEquals("foo", ((List<String>) result.get("properties")).get(0));
        });
    }

    @Test
    public void testIndexAndUniquenessConstraintOnNode() {
        db.execute("CREATE INDEX ON :Foo(foo)").close();
        db.execute("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.bar IS UNIQUE").close();
        testResult(db, "CALL apoc.schema.nodes()", (result) -> {
            Map<String, Object> r = result.next();

            assertEquals("Bar", r.get("label"));
            assertEquals("UNIQUENESS", r.get("type"));
            assertEquals("bar", ((List<String>) r.get("properties")).get(0));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("INDEX", r.get("type"));
            assertEquals("foo", ((List<String>) r.get("properties")).get(0));
            assertEquals("ONLINE", r.get("status"));

            assertFalse("Should not have another row",result.hasNext());
        });
    }

    /**
     * This test fails for a Community Edition
     */
    @Test
    public void testPropertyExistenceConstraintOnNode() {
        db.execute("CREATE CONSTRAINT ON (bar:Bar) ASSERT exists(bar.foobar)").close();
        testCall(db, "CALL apoc.schema.nodes()", (result) -> {

            assertEquals("Bar", result.get("label"));
            assertEquals("NODE_PROPERTY_EXISTENCE", result.get("type"));
            assertEquals(asList("foobar"), result.get("properties"));
        });
    }

    /**
     * This test fails for a Community Edition
     */
    @Test
    public void testConstraintExistsOnRelationship() {
        db.execute("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)").close();

        testResult(db, "RETURN apoc.schema.relationship.constraintExists('LIKED', ['day'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(true, r.entrySet().iterator().next().getValue());
        });
    }

    /**
     * This test fails for a Community Edition
     */
    @Test
    public void testSchemaRelationships() {
        db.execute("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)").close();
        testCall(db, "CALL apoc.schema.relationships()", (result) -> {
            assertEquals("CONSTRAINT ON ()-[liked:LIKED]-() ASSERT exists(liked.day)", result.get("name"));
            assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", result.get("type"));
            assertEquals(asList("day"), result.get("properties"));
            assertEquals(StringUtils.EMPTY, result.get("status"));
        });
    }

    /*
      This is only for 3.2+
    */
    @Test
    public void testDropCompoundIndexWhenUsingDropExisting() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar,baa)").close();
        testResult(db, "CALL apoc.schema.assert(null,null,true)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(0, indexes.size());
        }
    }

    /*
        This is only for 3.2+
    */
    @Test
    public void testDropCompoundIndexAndRecreateWithDropExisting() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar,baa)").close();
        testResult(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null,true)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    /*
        This is only for 3.2+
    */
    @Test
    public void testDoesntDropCompoundIndexWhenSupplyingSameCompoundIndex() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar,baa)").close();
        testResult(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null,false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }
    /*
        This is only for 3.2+
    */
    @Test
    public void testKeepCompoundIndex() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar,baa)").close();
        testResult(db, "CALL apoc.schema.assert({Foo:[['bar','baa'], ['foo','faa']]},null,false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "faa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(2, indexes.size());
        }
    }

    /*
        This is only for 3.2+
    */
    @Test
    public void testDropIndexAndCreateCompoundIndexWhenUsingDropExisting() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testResult(db, "CALL apoc.schema.assert({Bar:[['foo','bar']]},null)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    /*
        This is only for 3.2+
    */
    @Test
    public void testDropCompoundIndexAndCreateCompoundIndexWhenUsingDropExisting() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar,baa)").close();
        testResult(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar","baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    /*
        This is only for 3.2+
    */
    @Test
    public void testDropNodeKeyConstraintAndCreateNodeKeyConstraintWhenUsingDropExisting() throws Exception {
        db.execute("CREATE CONSTRAINT ON (f:Foo) ASSERT (f.bar,f.foo) IS NODE KEY").close();
        testResult(db, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar","foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(1, constraints.size());
        }
    }

    /*
        This is only for 3.2+
    */
    @Test
    public void testDropSchemaWithNodeKeyConstraintWhenUsingDropExisting() throws Exception {
        db.execute("CREATE CONSTRAINT ON (f:Foo) ASSERT (f.foo, f.bar) IS NODE KEY").close();
        testCall(db, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(0, constraints.size());
        }
    }

    @Test
    public void testDropConstraintExistsPropertyNode() throws Exception {
        db.execute("CREATE CONSTRAINT ON (m:Movie) ASSERT exists(m.title)").close();
        testCall(db, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Movie", r.get("label"));
            assertEquals(expectedKeys("title"), r.get("keys"));
            assertTrue("should be unique", (boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(0, constraints.size());
        }
    }

    @Test
    public void testDropConstraintExistsPropertyRelationship() throws Exception {
        db.execute("CREATE CONSTRAINT ON ()-[acted:Acted]->() ASSERT exists(acted.since)").close();
        testCall(db, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Acted", r.get("label"));
            assertEquals(expectedKeys("since"), r.get("keys"));
            assertTrue("should be unique", (boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(0, constraints.size());
        }
    }

    private List<String> expectedKeys(String... keys){
        return asList(keys);
    }
}
