package apoc.export.json.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;

public class CustomSerializeRelationship extends StdSerializer<Relationship> {

    public CustomSerializeRelationship() {
        this(null);
    }

    public CustomSerializeRelationship(Class<Relationship> t) {
        super(t);
    }

    @Override
    public void serialize(Relationship relationship, JsonGenerator jsonGenerator, SerializerProvider serializer) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("id", relationship.getId());
        jsonGenerator.writeStringField("type", relationship.getType().name());
        jsonGenerator.writeNumberField("start_node_id", relationship.getStartNodeId());
        jsonGenerator.writeNumberField("end_node_id", relationship.getEndNodeId());
        jsonGenerator.writeEndObject();
    }
}
