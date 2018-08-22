package apoc.load.relative;

import apoc.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoadXmlResult {

    public static String StringXmlNestedSimpleMap (){

        Map<String, Object> map = Util.map();

            map.put("_type", "parent");
            map.put("name", "databases");
            List<Object> objectList = new ArrayList<>();
            Map<String, Object> map1 = Util.map();
                map1.put("_type", "child");
                map1.put("name", "Neo4j");
                map1.put("_text", "Neo4j is a graph database");
            objectList.add(map1);
            Map<String, Object> map2 = Util.map();
                map2.put("_type", "child");
                map2.put("name", "relational");
                List<Object> objectList1 = new ArrayList<>();
                    Map<String, Object> map3 = Util.map();
                        map3.put("_type", "grandchild");
                        map3.put("name", "MySQL");
                        map3.put("_text", "MySQL is a database & relational");
                    Map<String, Object> map4 = Util.map();
                        map4.put("_type", "grandchild");
                        map4.put("name", "Postgres");
                        map4.put("_text", "Postgres is a relational database");
                    objectList1.add(map3);
                    objectList1.add(map4);
                map2.put("_children", objectList1);
            objectList.add(map2);
        map.put("_children", objectList);

        return map.toString();
    }

    public static String StringXmlNestedMap (){

        Map<String, Object> map = Util.map();

        map.put("_type", "parent");
        map.put("name", "databases");
        List<Object> objectList = new ArrayList<>();
            Map<String, Object> map1 = Util.map();
                map1.put("_type", "child");
                map1.put("name", "Neo4j");
                map1.put("_text", "Neo4j is a graph database");
            objectList.add(map1);
                Map<String, Object> map2 = Util.map();
                map2.put("_type", "child");
                map2.put("name", "relational");
            List<Object> objectList1 = new ArrayList<>();
            Map<String, Object> map3 = Util.map();
            map3.put("_type", "grandchild");
            map3.put("name", "MySQL");
            map3.put("_text", "MySQL is a database & relational");
            Map<String, Object> map4 = Util.map();
            map4.put("_type", "grandchild");
            map4.put("name", "Postgres");
            map4.put("_text", "Postgres is a relational database");
            objectList1.add(map3);
            objectList1.add(map4);
            map2.put("_grandchild", objectList1);
            objectList.add(map2);
        map.put("_child", objectList);

        return map.toString();
    }


    public static String StringXmlXPathNestedMap (){

        Map<String, Object> map = Util.map();
        List<Object> list = new ArrayList<>();
        map.put("_type", "book");
        map.put("id", "bk103");
            List<Object> objectList = new ArrayList<>();
            Map<String, Object> map1 = Util.map();
                map1.put("_type", "author");
                map1.put("_text", "Corets, Eva");
            Map<String, Object> map2 = Util.map();
                map2.put("_type", "title");
                map2.put("_text", "Maeve Ascendant");
            Map<String, Object> map3 = Util.map();
                map3.put("_type", "genre");
                map3.put("_text", "Fantasy");
            Map<String, Object> map4 = Util.map();
                map4.put("_type", "price");
                map4.put("_text", "5.95");
            Map<String, Object> map5 = Util.map();
                map5.put("_type", "publish_date");
                map5.put("_text", "2000-11-17");
            Map<String, Object> map6 = Util.map();
                map6.put("_type", "description");
                map6.put("_text", "After the collapse of a nanotechnology society in England, the young survivors lay the foundation for a new society.");
            objectList.add(map1);
            objectList.add(map2);
            objectList.add(map3);
            objectList.add(map4);
            objectList.add(map5);
            objectList.add(map6);
        map.put("_children", objectList);
        list.add(map);

        return list.toString();
    }
}
