package apoc.export.json;

import com.fasterxml.jackson.core.JsonGenerator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.spatial.Point;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public enum JsonFormatSerializer {

    DEFAULT() {
        @Override
        public void writeNode(JsonGenerator jsonGenerator, long id, Iterable<Label> labels, Map<String,Object> properties) throws IOException {

            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", "node");
            jsonGenerator.writeNumberField("id", id);
            jsonGenerator.writeArrayFieldStart("labels");
            for (Label label : labels) { jsonGenerator.writeString(label.toString()); }
            jsonGenerator.writeEndArray();
            serializeProperties(jsonGenerator, properties);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void writeRelationship(JsonGenerator jsonGenerator, long id, long startNodeId, long endNodeId, String type, Map<String,Object> properties) throws IOException {

            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", "relationship");
            jsonGenerator.writeNumberField("id", id);
            jsonGenerator.writeStringField("label", type);
            serializeProperties(jsonGenerator, properties);
            jsonGenerator.writeNumberField("start", startNodeId);
            jsonGenerator.writeNumberField("end", endNodeId);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeProperties(JsonGenerator jsonGenerator, Map<String, Object> properties) throws IOException {
            jsonGenerator.writeObjectFieldStart("data");
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                serializeProperty(jsonGenerator, key, value);
            }
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeProperty(JsonGenerator jsonGenerator, String key, Object value) throws IOException {

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

    };

    public abstract void writeNode(JsonGenerator jsonGenerator, long id, Iterable<Label> labels, Map<String,Object> properties) throws IOException;

    public abstract void writeRelationship(JsonGenerator jsonGenerator, long id, long startNodeId, long endNodeId, String type, Map<String,Object> properties) throws IOException;

    public abstract void serializeProperties(JsonGenerator jsonGenerator, Map<String,Object> properties) throws IOException;

    public abstract void serializeProperty(JsonGenerator jsonGenerator, String key, Object value) throws IOException;
}