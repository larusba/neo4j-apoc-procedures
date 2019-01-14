package apoc.refactor.util;

import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RefactorUtil {

    public static void mergeRelsWithSameTypeAndDirectionInMergeNodes(Node node, RefactorConfig config, Direction dir) {
        for (RelationshipType type : node.getRelationshipTypes()) {
            StreamSupport.stream(node.getRelationships(dir,type).spliterator(), false)
                    .collect(Collectors.groupingBy(rel -> Pair.of(rel.getStartNode(), rel.getEndNode())))
                    .values().stream()
                    .filter(list -> !list.isEmpty())
                    .forEach(list -> {
                        Relationship first = list.get(0);
                        for (int i = 1; i < list.size(); i++) {
                            Relationship relationship = list.get(i);
                            mergeRels(relationship, first, true,  config);
                        }
                    });
        }
    }

    public static void mergeRelsWithSameTypeAndDirectionInMergeNodesWithCount(Node node, RefactorConfig config, Direction dir) {
        for (RelationshipType type : node.getRelationshipTypes()) {
            StreamSupport.stream(node.getRelationships(dir,type).spliterator(), false)
                    .collect(Collectors.groupingBy(rel -> Pair.of(rel.getStartNode(), rel.getEndNode())))
                    .values().stream()
                    .filter(list -> !list.isEmpty())
                    .forEach(list -> {
                        Relationship first = list.get(0);
                        for (int i = 1; i < list.size(); i++) {
                            Relationship relationship = list.get(i);
                            if (config.isCountProperties()) {
                                int size = relationship.getAllProperties().size();
                                first.setProperty("count", (Integer) first.getProperty("count", size) + size);
                            }
                            mergeRels(relationship, first, true,  config);
                        }
                    });
        }
    }

    public static Relationship mergeRels(Relationship source, Relationship target, boolean delete, RefactorConfig conf) {
        Map<String, Object> properties = source.getAllProperties();
        if (delete) {
            source.delete();
        }
        PropertiesManager.mergeProperties(properties, target, conf);
        return target;
    }

    public static <T extends PropertyContainer> T copyProperties(PropertyContainer source, T target) {
        return copyProperties(source.getAllProperties(), target);
    }

    public static <T extends PropertyContainer> T copyProperties(Map<String, Object> source, T target) {
        for (Map.Entry<String, Object> prop : source.entrySet()) {
            target.setProperty(prop.getKey(), prop.getValue());
        }
        return target;
    }

}
