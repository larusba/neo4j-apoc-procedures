package apoc.export.json;

import apoc.Description;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.FileUtils.checkWriteAllowed;
import static apoc.util.FileUtils.getPrintWriter;

public class ExportJson {
    @Context
    public GraphDatabaseService db;

    public ExportJson(GraphDatabaseService db) {
        this.db = db;
    }

    public ExportJson() {
    }

    @Procedure
    @Description("apoc.export.json.all(file,config) - exports whole database as json to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(db), Util.relCount(db));
        return exportjson(fileName, source, new DatabaseSubGraph(db), config);
    }

    @Procedure
    @Description("apoc.export.json.data(nodes,rels,file,config) - exports given nodes and relationships as json to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportjson(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), config);
    }
    @Procedure
    @Description("apoc.export.json.graph(graph,file,config) - exports given graph object as json to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportjson(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), config);
    }

    @Procedure
    @Description("apoc.export.json.query(query,file,{config,...,params:{params}}) - exports results from the cypher kernelTransaction as json to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = db.execute(query,params);
        String source = String.format("kernelTransaction: cols(%d)", result.columns().size());
        return exportjson(fileName, source,result,config);
    }

    private Stream<ProgressInfo> exportjson(@Name("file") String fileName, String source, Object data, Map<String,Object> config) throws Exception {
        checkWriteAllowed();
        ExportConfig c = new ExportConfig(config);
        ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, source, "json"));
        PrintWriter printWriter = getPrintWriter(fileName, null);
        JsonFormat exporter = new JsonFormat(db);

        if (data instanceof SubGraph)
            exporter.dump(((SubGraph)data),printWriter,reporter,c);
        if (data instanceof Result)
            exporter.dump(((Result)data),printWriter,reporter,c);
        return reporter.stream();
    }
}

