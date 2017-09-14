package apoc.bolt;

import apoc.Description;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.UriResolver;
import apoc.util.Util;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class Bolt {

    @Context
    public GraphDatabaseService db;

    @Procedure()
    @Description("apoc.bolt.load(url-or-key, statement, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> load(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        if (params == null) params = Collections.emptyMap();
        if (config == null) config = Collections.emptyMap();
        boolean virtual = (boolean) config.getOrDefault("virtual", false);
        boolean addStatistics = (boolean) config.getOrDefault("statistics", false);
        boolean readOnly = (boolean) config.getOrDefault("readOnly", true);


        Config driverConfig = toDriverConfig(config.getOrDefault("driverConfig", map()));
        UriResolver uri = new UriResolver(url, "bolt");
        uri.initialize();

        //TODO driver routing
//        List<URI> uris = new ArrayList<>();
//        uris.add(uri.getConfiguredUri());
//        Driver routingDriver = GraphDatabase.routingDriver(uris, uri.getToken(), driverConfig);


        try (Driver driver = GraphDatabase.driver(uri.getConfiguredUri(), uri.getToken(), driverConfig);
             Session session = driver.session()) {
            if (addStatistics)
                return Stream.of(new RowResult(toMap(runStatement(statement, session, params, readOnly).summary().counters())));
            else
                return getRowResultStream(virtual, session, params, statement, readOnly);
        } catch (Exception e) {
            throw new RuntimeException("It's not possible to create a connection due to: " + e.getMessage());
        }
    }

    @Procedure()
    @Description("apoc.bolt.execute(url-or-key, statement, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> execute(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        Map<String, Object> configuration = new HashMap<>(config);
        configuration.put("readOnly", false);
        return load(url, statement, params, configuration);
    }

    private StatementResult runStatement(@Name("statement") String statement, Session session, Map<String, Object> finalParams, boolean read) {
        if (read) return session.readTransaction((Transaction tx) -> tx.run(statement, finalParams));
        else return session.writeTransaction((Transaction tx) -> tx.run(statement, finalParams));
    }

    private Stream<RowResult> getRowResultStream(boolean virtual, Session session, Map<String, Object> params, String statement, boolean read) {
        Map<Long, Object> nodesCache = new HashMap<>();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(runStatement(statement, session, params, read), 0), true)
                .map(record -> new RowResult(record.asMap(value -> {
                    Object entity = value.asObject();
                    if (entity instanceof Node) return toNode(entity, virtual, nodesCache);
                    if (entity instanceof Relationship) return toRelationship(entity, virtual, nodesCache);
                    if (entity instanceof Path) return toPath(entity, virtual, nodesCache);
                    return entity;
                })));
    }

    private Object toNode(Object value, boolean virtual, Map<Long, Object> nodesCache) {
        Value internalValue = ((InternalEntity) value).asValue();
        Node node = internalValue.asNode();
        if (virtual) {
            List<Label> labels = new ArrayList<>();
            node.labels().forEach(l -> labels.add(Label.label(l)));
            VirtualNode virtualNode = new VirtualNode(node.id(), labels.toArray(new Label[0]), node.asMap(), db);
            nodesCache.put(node.id(), virtualNode);
            return virtualNode;
        } else
            return Util.map("entityType", internalValue.type().name(), "labels", node.labels(), "id", node.id(), "properties", node.asMap());
    }

    private Object toRelationship(Object value, boolean virtual, Map<Long, Object> nodesCache) {
        Value internalValue = ((InternalEntity) value).asValue();
        Relationship relationship = internalValue.asRelationship();
        if (virtual) {
            VirtualNode start = (VirtualNode) nodesCache.getOrDefault(relationship.startNodeId(), new VirtualNode(relationship.startNodeId(), db));
            VirtualNode end = (VirtualNode) nodesCache.getOrDefault(relationship.endNodeId(), new VirtualNode(relationship.endNodeId(), db));
            VirtualRelationship virtualRelationship = new VirtualRelationship(relationship.id(), start, end, RelationshipType.withName(relationship.type()), relationship.asMap());
            return virtualRelationship;
        } else
            return Util.map("entityType", internalValue.type().name(), "type", relationship.type(), "id", relationship.id(), "start", relationship.startNodeId(), "end", relationship.endNodeId(), "properties", relationship.asMap());
    }

    private Object toPath(Object value, boolean virtual,  Map<Long, Object> nodesCache) {
        List<Object> entityList = new LinkedList<>();
        Value internalValue = ((InternalPath) value).asValue();
        Path path = internalValue.asPath();
        path.forEach(p-> {
            if (virtual) {
                VirtualNode startNode = (VirtualNode) toNode(p.start(), virtual, nodesCache);
                VirtualNode endNode = (VirtualNode) toNode(p.end(), virtual, nodesCache);
                entityList.add(startNode);
                entityList.add(new VirtualRelationship(p.relationship().id(), startNode, endNode, RelationshipType.withName(p.relationship().type()), p.relationship().asMap()));
                entityList.add(endNode);
            } else
                entityList.add(Util.map("entityType", "NODE", "startLabels", p.start().labels(), "startId", p.start().id(), "type", p.relationship().type(), "endLabels", p.end().labels(),
                        "endId", p.end().id(), "startProperties", p.start().asMap(), "relProperties", p.relationship().asMap(), "endProperties", p.end().asMap()));

        });
        return entityList;
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

    private Config toDriverConfig(Object driverConfig) {
        Map<String, Object> driverConfMap = (Map<String, Object>) driverConfig;
        Object ttl = driverConfMap.getOrDefault("ttl", "");
        Object logging = driverConfMap.getOrDefault("logging", "");
        Config config1 = Config.build().withoutEncryption().toConfig();
        Config config = Config.build().toConfig();//withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig();
        System.out.println("config.logging() = " + config.logging().getLog("logging"));
        return config;
    }
}
