package apoc.load;

import apoc.Description;
import apoc.result.*;
import apoc.util.UriCreator;
import apoc.util.Util;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.types.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author AgileLARUS
 * @since 03.09.17
 */
public class Bolt {
    @Context
    public GraphDatabaseService db;

    @Procedure()
    @Description("apoc.load.bolt(url, statement, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> bolt(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws MalformedURLException {

        boolean virtual = (boolean) config.getOrDefault("virtual", false);
        UriCreator uri = new UriCreator(url, config).invoke();
        Driver driver = GraphDatabase.driver(uri.getUrlDriver(), AuthTokens.basic(uri.getUser(), uri.getPassword()));
        Session session = driver.session();
        List<Record> records = session.readTransaction((Transaction tx) -> tx.run(statement, params).list());
        Stream<RowResult> results = records.stream()
                .map(record -> record.fields())
                .map(keyValueList -> {
                    Map<String, Object> map = new HashMap<>();
                    for (int i = 0; i < keyValueList.size(); i++) {
                        String key = keyValueList.get(i).key();
                        Object value = keyValueList.get(i).value().asObject();
                        if (!(value instanceof InternalPath) && !(value instanceof InternalEntity))
                            map.putAll(Util.map(key, value));
                        else {
                            getPath(map, value, virtual);
                            getEntity(value, map, key, virtual);
                        }
                    }
                    return map;
                })
                .map(resultMap -> new RowResult(resultMap));
        session.close();
        driver.close();

        return results;
    }

    private void getEntity(Object value, Map<String, Object> map, String key, boolean virtual) {
        Map<String, Object> mapTemp = new HashMap<>();
        if (value instanceof InternalEntity) {
            Value internalValue = ((InternalEntity) value).asValue();
            if (internalValue instanceof NodeValue) {
                Node node = internalValue.asNode();
                if (virtual) {
                    VirtualNode virtualNode = new VirtualNode(node.id(), db);
                    node.labels().forEach(l -> {
                        virtualNode.addLabel(Label.label(l));
                    });
                    node.asMap().entrySet().iterator().forEachRemaining(i -> virtualNode.setProperty(i.getKey(), i.getValue()));
                    map.put(key, virtualNode);
                } else {
                    mapTemp = new HashMap<>();
                    mapTemp.putAll(Util.map("entityType", internalValue.type().name(), "labels", node.labels(), "id", node.id()));
                    mapTemp.put("properties", node.asMap());
                    map.put(key, mapTemp);
                }
            }
            if (internalValue instanceof RelationshipValue) {
                Relationship relationship = internalValue.asRelationship();
                if (virtual) {
                    VirtualNode start = new VirtualNode(relationship.startNodeId(), db);
                    VirtualNode end = new VirtualNode(relationship.endNodeId(), db);

                    if(map.values().contains(start) && map.values().contains(end))
                        return;
                    VirtualRelationship virtualRelationship = new VirtualRelationship(start, end, RelationshipType.withName(relationship.type()));
                    relationship.asMap().entrySet().iterator().forEachRemaining(i -> virtualRelationship.setProperty(i.getKey(), i.getValue()));
                    map.put(key, virtualRelationship);
                } else {
                    mapTemp.putAll(Util.map("entityType", internalValue.type().name(), "type", relationship.type(), "id", relationship.id(), "start", relationship.startNodeId(), "end", relationship.endNodeId()));
                    mapTemp.put("properties", relationship.asMap());
                    map.put(key, mapTemp);
                }
            }
        }
    }

    private void getPath(Map<String, Object> map, Object value, boolean virtual) {
        if (value instanceof InternalPath) {
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
                    map.putAll(Util.map("start", startNode, "end", endNode));
                } else {
                    map.putAll(Util.map("entityType", "NODE", "startLabels", segment.start().labels(), "startId", segment.start().id(), "type", segment.relationship().type(), "endLabels", segment.end().labels(), "endId", segment.end().id()));
                    map.put("startProperties", segment.start().asMap());
                    map.put("relProperties", segment.relationship().asMap());
                    map.put("endProperties", segment.end().asMap());
                }
            });
        }
    }
}
