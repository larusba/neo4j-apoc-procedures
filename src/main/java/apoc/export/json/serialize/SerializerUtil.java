package apoc.export.json.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import org.neo4j.graphdb.PropertyContainer;

import java.io.IOException;

public class SerializerUtil {

    public static void serializerProperty(JsonGenerator jsonGenerator, PropertyContainer propertyContainer) throws IOException {
        jsonGenerator.writeObjectFieldStart("properties");
        propertyContainer.getAllProperties().forEach((key, value)-> {
            try {
                if(value.getClass().isArray()){
                    String[] prop;
                    jsonGenerator.writeArrayFieldStart(key);
                    for(String property : prop = (String[]) value){
                        jsonGenerator.writeString(property);
                    }
                    jsonGenerator.writeEndArray();
                } else {
                    jsonGenerator.writeStringField(key, value.toString());
                }
            } catch (IOException e) {}
        });
        jsonGenerator.writeEndObject();
    }

}
