package apoc.bolt;

import apoc.Description;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.UriResolver;
import apoc.util.Util;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.types.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class Bolt {

    @Context public GraphDatabaseService db;

    @Procedure()
    @Description("apoc.bolt.load(url, statement, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> load(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {

        if (params == null) params = Collections.emptyMap();
        if (config == null) config = Collections.emptyMap();
        boolean virtual = (boolean) config.getOrDefault("virtual", false);
        boolean addStatistics = (boolean) config.getOrDefault("statistics", false);
        UriResolver uri = new UriResolver(url, config);
        Driver driver = null;
        Session session = null;
        try {
            driver = GraphDatabase.driver(uri.getUrlDriver(), AuthTokens.basic(uri.getUser(), uri.getPassword()));
            session = driver.session();
            if (addStatistics) return Stream.of(new RowResult(toMap(runStatement(statement, session, params).summary().counters())));

            return getRowResultStream(virtual, session, params, statement);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            session.close();
            driver.close();
        }
    }

    @Procedure()
    @Description("apoc.bolt.execute(url, statement, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> execute(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        config.replace("virtual",false);
        return load(url, statement, params, config);
    }

    private StatementResult runStatement(@Name("statement") String statement, Session session, Map<String, Object> finalParams) {
        return session.writeTransaction((Transaction tx) -> tx.run(statement, finalParams));
    }

    private Stream<RowResult> getRowResultStream(boolean virtual, Session session, Map<String, Object> params, String statement) {
        return runStatement(statement, session, params).list().stream()
                .map(record -> record.fields())
                .map(fields -> {
                    Map<String, Object> map = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        String key = fields.get(i).key();
                        Object value = fields.get(i).value().asObject();
                        if (value instanceof Path) map.putAll(toPath(value, virtual, key));
                        else if (value instanceof Node) map.putAll(toNode(value, key, virtual));
                        else if (value instanceof Relationship) map.putAll(toRelationship(value, map, key, virtual));
                        else map.put(key, value);
                    }
                    return map;
                })
                .map(resultMap -> new RowResult(resultMap));
    }

    private Map<String, Object> toNode(Object value, String key, boolean virtual) {
        Value internalValue = ((InternalEntity) value).asValue();
        Node node = internalValue.asNode();
        if (virtual) {
            VirtualNode virtualNode = new VirtualNode(node.id(), db);
            node.labels().forEach(l -> virtualNode.addLabel(Label.label(l)));
            node.asMap().entrySet().iterator().forEachRemaining(i -> virtualNode.setProperty(i.getKey(), i.getValue()));
            return Util.map(key, virtualNode);
        } else
            return Util.map(key, Util.map("entityType", internalValue.type().name(), "labels", node.labels(), "id", node.id(), "properties", node.asMap()));
    }

    private Map<String, Object> toRelationship(Object value, Map<String, Object> map, String key, boolean virtual) {
        Value internalValue = ((InternalEntity) value).asValue();
        Relationship relationship = internalValue.asRelationship();
        if (virtual) {
            VirtualNode start = new VirtualNode(relationship.startNodeId(), db);
            VirtualNode end = new VirtualNode(relationship.endNodeId(), db);
            if (map.values().contains(start) && map.values().contains(end))
                return map;
            VirtualRelationship virtualRelationship = new VirtualRelationship(start, end, RelationshipType.withName(relationship.type()));
            relationship.asMap().entrySet().iterator().forEachRemaining(i -> virtualRelationship.setProperty(i.getKey(), i.getValue()));
            return Util.map(key, virtualRelationship);
        } else
            return Util.map(key, Util.map("entityType", internalValue.type().name(), "type", relationship.type(), "id", relationship.id(), "start", relationship.startNodeId(), "end", relationship.endNodeId(), "properties", relationship.asMap()));
    }

    private Map<String, Object> toPath(Object value, boolean virtual, String key) {
        Map<String, Object> map = new HashMap<>();
        InternalPath value1 = (InternalPath) value;
        Path path = value1.asValue().asPath();
        path.spliterator().forEachRemaining(segment -> {
            if (virtual) {
                VirtualNode startNode = new VirtualNode(segment.start().id(), db);
                segment.start().labels().forEach(l -> startNode.addLabel(Label.label(l)));
                segment.start().asMap().entrySet().iterator().forEachRemaining(p -> startNode.setProperty(p.getKey(), p.getValue()));
                VirtualNode endNode = new VirtualNode(segment.end().id(), db);
                segment.end().labels().forEach(l -> endNode.addLabel(Label.label(l)));
                segment.end().asMap().entrySet().iterator().forEachRemaining(p -> endNode.setProperty(p.getKey(), p.getValue()));
                map.put(key, Util.map("start", startNode, "end", endNode));
            } else
                map.put(key, Util.map("entityType", "NODE", "startLabels", segment.start().labels(), "startId", segment.start().id(), "type", segment.relationship().type(), "endLabels", segment.end().labels(),
                        "endId", segment.end().id(), "startProperties", segment.start().asMap(), "relProperties", segment.relationship().asMap(), "endProperties", segment.end().asMap()));
        });
        return map;
    }

    private Map<String, Object> toMap(SummaryCounters resultSummary) {
        return map(
                "nodesCreated", resultSummary.nodesCreated(),
                "nodesDeleted", resultSummary.nodesDeleted(),
                "labelsAdded", resultSummary.labelsAdded(),
                "labelsRemoved", resultSummary.labelsRemoved(),
                "relationshipsCreated", resultSummary.relationshipsCreated(),
                "relationshipsDeleted", resultSummary.relationshipsDeleted(),
                "propertiesSet", resultSummary.propertiesSet(),
                "constraintsAdded", resultSummary.constraintsAdded(),
                "constraintsRemoved", resultSummary.constraintsRemoved(),
                "indexesAdded", resultSummary.indexesAdded(),
                "indexesRemoved", resultSummary.indexesRemoved()
        );
    }

}
