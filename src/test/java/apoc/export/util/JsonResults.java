package apoc.export.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonResults {

    public static Map<String, Object> nodeFoo(boolean datatypes) throws JsonProcessingException {

        Map<String, Object> map = new LinkedHashMap<String, Object>() {{
            put("type", "node");
            put("id", 0);
            put("labels", Arrays.asList("User"));
            Map<String, Object> data = new LinkedHashMap<String, Object>() {{
                put("name", "foo");
                put("age", 42);
                put("male", true);
                put("kids", Arrays.asList("a", "b", "c"));
            }};
            put("data", data);
            if(datatypes) {
                Map<String, Object> datatypes = new LinkedHashMap<String, Object>() {{
                    put("name", "string");
                    put("male", "boolean");
                    put("age", "long");
                    put("kids", "string[]");
                }};
                put("data_types",datatypes);
            }
        }};

        return map;
    }

    public static Map<String, Object> nodeBar(boolean datatypes) throws JsonProcessingException {

        Map<String, Object> map = new LinkedHashMap<String, Object>() {{
            put("type", "node");
            put("id", 1);
            put("labels", Arrays.asList("User"));
            Map<String, Object> data = new LinkedHashMap<String, Object>() {{
                put("name", "bar");
                put("age", 42);
            }};
            put("data", data);
            if(datatypes) {
                Map<String,Object> datatypes = new LinkedHashMap<String,Object>(){{
                    put("name", "string");
                    put("age", "long");
                }};
                put("data_types",datatypes);
            }
        }};

        return map;
    }

    public static Map<String, Object> nodeAge(boolean datatypes) throws JsonProcessingException {

        Map<String, Object> map = new LinkedHashMap<String, Object>() {{
            put("type", "node");
            put("id", 2);
            put("labels", Arrays.asList("User"));
            Map<String, Object> data = new LinkedHashMap<String, Object>() {{
                put("age", 12);
            }};
            put("data", data);
            if(datatypes) {
                Map<String,Object> datatypes = new LinkedHashMap<String,Object>(){{
                    put("age", "long");
                }};
                put("data_types",datatypes);
            }
        }};

        return map;
    }

    public static Map<String, Object> relationship() throws JsonProcessingException {

        Map<String, Object> map = new LinkedHashMap<String, Object>() {{
            put("type", "relationship");
            put("id", 0);
            put("label", "KNOWS");
            put("start", 0);
            put("end", 1);
        }};

        return map;
    }

    public static String expectedAllGraphData() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(nodeFoo(false))+"\n"
                +objectMapper.writeValueAsString(nodeBar(false))+"\n"
                +objectMapper.writeValueAsString(nodeAge(false))+"\n"
                +objectMapper.writeValueAsString(relationship());
    }

    public static String expectedMapPointDatetime() throws JsonProcessingException {

        Map<String, Object> dataValues = new LinkedHashMap<String, Object>() {{

            Map<String, Object> map = new LinkedHashMap<String, Object>() {
                {
                    put("a", 1);
                    Map<String, Object> b = new LinkedHashMap<String, Object>() {{
                        put("c", 1);
                        put("d", "a");
                        Map<String, Object> e = new LinkedHashMap<String, Object>() {{
                            put("f", Arrays.asList(1, 3, 5));
                        }};
                        put("e",e);
                    }};
                    put("b", b);
                }};
            put("map",map);
            put("theDateTime", "2015-06-24T12:50:35.556+01:00");
            put("theLocalDateTime", "2015-07-04T19:32:24");

            Map<String, Object> point = new LinkedHashMap<String, Object>() {{
                Map<String, Object> crs = new LinkedHashMap<String, Object>() {{
                    put("name","wgs-84");
                    put("table","EPSG");
                    put("code",4326);
                    put("href","http://spatialreference.org/ref/epsg/4326/");
                    put("dimension",2);
                    put("geographic",true);
                    put("calculator",new LinkedHashMap<>());
                    put("type","wgs-84");
                }};
                put("crs", crs);
                put("coordinates", Arrays.asList(33.46789,13.1));
            }};
            put("point",point);

            put("date","2015-03-26");
            put("time","12:50:35.556+01:00");
            put("localTime","12:50:35.556");

        }};

        Map<String, Object> data = new LinkedHashMap<String, Object>() {{
            put("data",dataValues);
        }};

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(data);
    }

    public static String expectedQueryNodesCount() throws JsonProcessingException {

        Map<String, Object> data = new LinkedHashMap<String, Object>() {{
            Map<String, Object> map = new LinkedHashMap<String, Object>() {{
                put("count", 3);
            }};
            put("data",map);
        }};

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(data);
    }

    public static String expectedQueryTwoNodes() throws JsonProcessingException {

        Map<String, Object> map = new LinkedHashMap<String, Object>() {{
            put("u", nodeFoo(false));
            put("l", nodeBar(false));
        }};

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(map);
    }

    public static String expectedQueryNodes() throws JsonProcessingException {

        Map<String, Object> node = new LinkedHashMap<String, Object>() {{
            put("u", nodeFoo(false));
        }};
        Map<String, Object> node2 = new LinkedHashMap<String, Object>() {{
            put("u", nodeBar(false));
        }};
        Map<String, Object> node3 = new LinkedHashMap<String, Object>() {{
            put("u", nodeAge(false));
        }};

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(node)+"\n"+
                objectMapper.writeValueAsString(node2)+"\n"+
                objectMapper.writeValueAsString(node3);
    }

    public static String expectedQueryPath() throws JsonProcessingException {

        Map<String, Object> map = new LinkedHashMap<String, Object>() {{
            put("u", nodeFoo(false));
            put("rel", relationship());
            put("u2", nodeBar(false));
            Map<String, Object> path = new LinkedHashMap<String, Object>() {{
                put("startNode", nodeFoo(false));
                put("relationship", relationship());
                put("endNode", nodeBar(false));
            }};
            put("p",path);
            put("name","foo");
        }};

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(map);
    }

    public static String expectedQueryData() throws JsonProcessingException {

        Map<String, Object> data = new LinkedHashMap<String, Object>() {{
            Map<String, Object> map = new LinkedHashMap<String, Object>() {{
                put("age", 42);
                put("name", "foo");
                put("male", true);
                put("kids", Arrays.asList("a", "b", "c"));
                put("labels", Arrays.asList("User"));
            }};
            put("data", map);
        }};

        Map<String, Object> data2 = new LinkedHashMap<String, Object>() {{
            Map<String, Object> map2 = new LinkedHashMap<String, Object>() {{
                put("age", 42);
                put("name", "bar");
                put("male", null);
                put("kids", null);
                put("labels", Arrays.asList("User"));
                }};
            put("data", map2);
        }};

        Map<String, Object> data3 = new LinkedHashMap<String, Object>() {{
            Map<String, Object> map3 = new LinkedHashMap<String, Object>() {{
                put("age", 12);
                put("name", null);
                put("male", null);
                put("kids", null);
                put("labels", Arrays.asList("User"));
                }};
            put("data", map3);
        }};

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(data)+"\n"+
                objectMapper.writeValueAsString(data2)+"\n"+
                objectMapper.writeValueAsString(data3);
    }

    public static Map<String, Object> dataTypesAge(){
        Map<String,Object> datatypes = new LinkedHashMap<String,Object>(){{
                put("age", "long");
        }};

        return datatypes;
    }

    public static String expectedAllWithDataTypes() throws JsonProcessingException{
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(nodeFoo(true))+"\n"
                +objectMapper.writeValueAsString(nodeBar(true))+"\n"
                +objectMapper.writeValueAsString(nodeAge(true))+"\n"
                +objectMapper.writeValueAsString(relationship());
    }

}
