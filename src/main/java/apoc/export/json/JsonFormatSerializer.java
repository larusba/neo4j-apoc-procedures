package apoc.export.json;

import apoc.export.util.ExportConfig;
import com.fasterxml.jackson.core.JsonGenerator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.spatial.Point;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;

public enum JsonFormatSerializer {

    DEFAULT() {
        @Override
        public void writeNode(JsonGenerator jsonGenerator, long id, Iterable<Label> labels, Map<String,Object> properties, ExportConfig config) throws IOException {

            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", "node");
            jsonGenerator.writeNumberField("id", id);
            if(labels.iterator().hasNext()) {
                jsonGenerator.writeArrayFieldStart("labels");
                for (Label label : labels) {
                    jsonGenerator.writeString(label.toString());
                }
            }
            jsonGenerator.writeEndArray();
            serializeProperties(jsonGenerator, properties, config);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void writeRelationship(JsonGenerator jsonGenerator, long id, long startNodeId, long endNodeId, String type, Map<String,Object> properties, ExportConfig config) throws IOException {

            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", "relationship");
            jsonGenerator.writeNumberField("id", id);
            jsonGenerator.writeStringField("label", type);
            serializeProperties(jsonGenerator, properties, config);
            jsonGenerator.writeNumberField("start", startNodeId);
            jsonGenerator.writeNumberField("end", endNodeId);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeProperties(JsonGenerator jsonGenerator, Map<String, Object> properties, ExportConfig config) throws IOException {
            if(properties != null && !properties.isEmpty()) {
                jsonGenerator.writeObjectFieldStart("data");
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    serializeProperty(jsonGenerator, key, value);
                }
                jsonGenerator.writeEndObject();
                if (config.useTypes()) {
                    writeDataTypes(jsonGenerator, properties);
                }
            }
        }

        @Override
        public void serializeProperty(JsonGenerator jsonGenerator, String key, Object value) throws IOException {

            key = clearKey(key);

            if (value == null) {
                jsonGenerator.writeNullField(key);
            } else if (value.getClass().isArray()) {
                    jsonGenerator.writeArrayFieldStart(key);
                    int length = Array.getLength(value);
                    for (int i = 0; i < length; i++) {
                        jsonGenerator.writeObject(Array.get(value, i));
                    }
                    jsonGenerator.writeEndArray();
                } else if (value instanceof TemporalAccessor) {
                    jsonGenerator.writeStringField(key, value.toString());
                } else if (value instanceof Point) {
                    Point point = (Point) value;
                    Map<String, Object> pointMap = new HashMap<>();
                    pointMap.put("coordinates", point.getCoordinate().getCoordinate());
                    pointMap.put("crs", point.getCRS());
                    jsonGenerator.writeObjectField(key, pointMap);
                } else {
                    jsonGenerator.writeObjectField(key, value);
                }
        }

        private String clearKey(String key){
            if(key.contains("."))
                key = key.split("\\.")[1];
            if(key.contains("("))
                key = key.split("\\(")[0];
            return key;
        }

        @Override
        public void writeDataTypes(JsonGenerator jsonGenerator, Map<String, Object> properties) throws IOException {
            Map<String, String> dataTypes = properties.entrySet().stream()
                    .collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue().getClass().getSimpleName().toLowerCase()));
            jsonGenerator.writeObjectField("data_types",dataTypes);
        }

    };

    public abstract void writeNode(JsonGenerator jsonGenerator, long id, Iterable<Label> labels, Map<String,Object> properties, ExportConfig config) throws IOException;

    public abstract void writeRelationship(JsonGenerator jsonGenerator, long id, long startNodeId, long endNodeId, String type, Map<String,Object> properties, ExportConfig config) throws IOException;

    public abstract void serializeProperties(JsonGenerator jsonGenerator, Map<String,Object> properties, ExportConfig config) throws IOException;

    public abstract void serializeProperty(JsonGenerator jsonGenerator, String key, Object value) throws IOException;

    public abstract void writeDataTypes(JsonGenerator jsonGenerator, Map<String, Object> properties) throws IOException;
}