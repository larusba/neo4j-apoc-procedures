package apoc.export.json;

import apoc.export.util.ExportConfig;
import com.fasterxml.jackson.core.JsonGenerator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.util.Map;

public enum JsonFormatSerializer {

    DEFAULT() {

        @Override
        public void writeNode(JsonGenerator jsonGenerator, Node node, ExportConfig config) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("id", node.getId());
            jsonGenerator.writeStringField("type", "node");
            writeNodeDetails(jsonGenerator, node, true, config);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void writeRelationship(JsonGenerator jsonGenerator, Relationship rel, boolean writeNodeProperties, ExportConfig config) throws IOException {
            Node startNode = rel.getStartNode();
            Node endNode = rel.getEndNode();
            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("id", rel.getId());
            jsonGenerator.writeStringField("type", "relationship");
            jsonGenerator.writeStringField("label", rel.getType().toString());
            serializeProperties(jsonGenerator, rel.getAllProperties(), config);
            writeRelationshipNode(jsonGenerator, "start", startNode, writeNodeProperties, config);
            writeRelationshipNode(jsonGenerator, "end", endNode, writeNodeProperties, config);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void writeRelationshipNode(JsonGenerator jsonGenerator, String type, Node node, boolean writeProperties, ExportConfig config) throws IOException {
            jsonGenerator.writeObjectFieldStart(type);
            jsonGenerator.writeNumberField("id", node.getId());
            writeNodeDetails(jsonGenerator, node, writeProperties, config);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeProperties(JsonGenerator jsonGenerator, Map<String, Object> properties, ExportConfig config) throws IOException {
            if(properties != null && !properties.isEmpty()) {
                jsonGenerator.writeObjectFieldStart("properties");
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    serializeProperty(jsonGenerator, key, value, true);
                }
                jsonGenerator.writeEndObject();
            }
        }

        @Override
        public void serializeProperty(JsonGenerator jsonGenerator, String key, Object value, boolean writeKey) throws IOException {
            if (value == null) {
                if (writeKey) {
                    jsonGenerator.writeNullField(key);
                } else {
                    jsonGenerator.writeNull();
                }
            } else {
                if (writeKey) {
                    jsonGenerator.writeObjectField(key, value);
                } else {
                    jsonGenerator.writeObject(value);
                }
            }
        }

        private void writeNodeDetails(JsonGenerator jsonGenerator, Node node, boolean writeProperties, ExportConfig config) throws IOException {
            Iterable<Label> labels = node.getLabels();
            if (labels.iterator().hasNext()) {
                jsonGenerator.writeArrayFieldStart("labels");
                for (Label label : labels) {
                    jsonGenerator.writeString(label.toString());
                }
                jsonGenerator.writeEndArray();
            }
            if (writeProperties) {
                serializeProperties(jsonGenerator, node.getAllProperties(), config);
            }
        }
    };



    public abstract void writeNode(JsonGenerator jsonGenerator, Node node, ExportConfig config) throws IOException;

    public abstract void writeRelationship(JsonGenerator jsonGenerator, Relationship relationship, boolean writeNodeProperties, ExportConfig config) throws IOException;

    public abstract void serializeProperties(JsonGenerator jsonGenerator, Map<String,Object> properties, ExportConfig config) throws IOException;

    public abstract void serializeProperty(JsonGenerator jsonGenerator, String key, Object value, boolean writeKey) throws IOException;

    public abstract void writeRelationshipNode(JsonGenerator jsonGenerator, String type, Node node, boolean writeNodeProperties, ExportConfig config) throws IOException;

}