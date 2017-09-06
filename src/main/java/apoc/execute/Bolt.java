package apoc.execute;

import apoc.Description;
import apoc.result.RowResult;
import apoc.util.UriCreator;
import apoc.util.Util;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class Bolt {

    @Context
    public GraphDatabaseService db;

    @Procedure()
    @Description("apoc.execute.bolt(url, statement, params, config) - access to other databases via bolt for read and write")
    public Stream<RowResult> bolt(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws MalformedURLException {

        boolean addStatistics = (boolean) config.getOrDefault("statistics", false);
        UriCreator getUri = new UriCreator(url, config).invoke();
        Driver driver = GraphDatabase.driver(getUri.getUrlDriver(), AuthTokens.basic(getUri.getUser(), getUri.getPassword()));
        Session session = driver.session();

        if (addStatistics) {
            SummaryCounters resultSummary = session.writeTransaction((Transaction tx) -> tx.run(statement, params).summary()).counters();
            return Stream.of(new RowResult(toMap(resultSummary)));
        }

        List<Record> records = session.writeTransaction((Transaction tx) -> tx.run(statement, params).list());
        Stream<RowResult> results = records.stream()
                .map(record -> record.fields())
                .map(keyValueList -> {
                    Map<String, Object> map = new HashMap<>();
                    for (int i = 0; i < keyValueList.size(); i++) {
                        String key = keyValueList.get(i).key();
                        Object value = keyValueList.get(i).value().asObject();
                        if (!(value instanceof InternalPath) && !(value instanceof InternalEntity))
                            map.putAll(Util.map(key, value));
                        getPath(map, value);
                        getEntity(map, value, key);
                    }
                    return map;
                })
                .map(resultMap -> new RowResult(resultMap));

        session.close();
        driver.close();

        return results;
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

    private void getEntity(Map<String, Object> map, Object value, String key) {
        Map<String, Object> mapTemp = new HashMap<>();
        if (value instanceof InternalEntity) {
            Value internalValue = ((InternalEntity) value).asValue();
            if (internalValue instanceof NodeValue) {
                Node node = internalValue.asNode();
                mapTemp = new HashMap<>();
                mapTemp.putAll(Util.map("entityType", internalValue.type().name(), "labels", node.labels(), "id", node.id()));
                mapTemp.put("properties", node.asMap());
                map.put(key, mapTemp);
            } else if (internalValue instanceof RelationshipValue) {
                Relationship relationship = internalValue.asRelationship();
                mapTemp.putAll(Util.map("entityType", internalValue.type().name(), "type", relationship.type(), "id", relationship.id(), "start", relationship.startNodeId(), "end", relationship.endNodeId()));
                mapTemp.put("properties", relationship.asMap());
                map.put(key, mapTemp);
            }
        }
    }

    private void getPath(Map<String, Object> map, Object value) {
        if (value instanceof InternalPath) {
            InternalPath value1 = (InternalPath) value;
            Path path = value1.asValue().asPath();
            path.spliterator().forEachRemaining(segment -> {
                map.putAll(Util.map("entityType", "NODE", "startLabels", segment.start().labels(), "startId", segment.start().id(), "type", segment.relationship().type(), "endLabels", segment.end().labels(), "endId", segment.end().id()));
                map.put("startProperties", segment.start().asMap());
                map.put("relProperties", segment.relationship().asMap());
                map.put("endProperties", segment.end().asMap());
            });
        }
    }
}
