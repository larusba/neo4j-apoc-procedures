package apoc.export.util;

import apoc.export.cypher.formatter.CypherFormat;
import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

/**
 * @author mh
 * @since 19.01.14
 */
public class ExportConfig {
    public static final char QUOTECHAR = '"';
    public static final int DEFAULT_BATCH_SIZE = 20000;
    private static final int DEFAULT_UNWIND_BATCH_SIZE = 100;
    public static final String DEFAULT_DELIM = ",";
    public static final String DEFAULT_QUOTES_TYPE = "always";
    private final boolean streamStatements;

    public enum OptimizationType {NONE, UNWIND_BATCH};

    private OptimizationType optimizationType;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int unwindBatchSize = DEFAULT_UNWIND_BATCH_SIZE;
    private boolean silent = false;
    private String delim = DEFAULT_DELIM;
    private String quotes = "always";
    private boolean useTypes = false;
    private boolean writeNodeProperties = false;
    private boolean nodesOfRelationships;
    private ExportFormat format;
    private CypherFormat cypherFormat;
    private final Map<String, Object> config;
    private Map<String, Object> optimizations;

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isSilent() {
        return silent;
    }

    public char getDelimChar() {
        return delim.charAt(0);
    }

    public String getDelim() {
        return delim;
    }

    public String isQuotes() {
        return quotes;
    }

    public boolean useTypes() {
        return useTypes;
    }

    public ExportFormat getFormat() { return format; }

    public int getUnwindBatchSize() {
        return ((Number)getOptimizations().getOrDefault("batchSize", DEFAULT_UNWIND_BATCH_SIZE)).intValue();
    }

    public CypherFormat getCypherFormat() {
        return cypherFormat;
    }

    public ExportConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.silent = toBoolean(config.getOrDefault("silent",false));
        this.batchSize = ((Number)config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE)).intValue();
        this.delim = delim(config.getOrDefault("d", String.valueOf(DEFAULT_DELIM)).toString());
        this.useTypes = toBoolean(config.get("useTypes"));
        this.nodesOfRelationships = toBoolean(config.get("nodesOfRelationships"));
        this.format = ExportFormat.fromString((String) config.getOrDefault("format", "neo4j-shell"));
        this.cypherFormat = CypherFormat.fromString((String) config.getOrDefault("cypherFormat", "create"));
        this.config = config;
        this.optimizations = (Map<String, Object>) config.getOrDefault("useOptimizations", Util.map());
        this.streamStatements = toBoolean(config.get("streamStatements")) || toBoolean(config.get("stream"));
        this.writeNodeProperties = toBoolean(config.get("writeNodeProperties"));
        this.optimizationType = OptimizationType.valueOf(optimizations.getOrDefault("type", OptimizationType.UNWIND_BATCH.toString()).toString().toUpperCase());
        exportQuotes(config);
    }

    private void exportQuotes(Map<String, Object> config)
    {
        try {
            this.quotes = (String) config.getOrDefault("quotes",DEFAULT_QUOTES_TYPE);
            if (quotes == null) {
                quotes = DEFAULT_QUOTES_TYPE;
            }
        } catch (ClassCastException e) { // backward compatibility
            this.quotes = toBoolean(config.get("quotes")) ? "always" : "none";
        }
    }

    public boolean getRelsInBetween() {
        return nodesOfRelationships;
    }

    private static String delim(String value) {
        if (value.length()==1) return value;
        if (value.contains("\\t")) return String.valueOf('\t');
        if (value.contains(" ")) return " ";
        throw new RuntimeException("Illegal delimiter '"+value+"'");
    }

    public ExportConfig withTypes() {
        this.useTypes =true;
        return this;
    }

    public String defaultRelationshipType() {
        return config.getOrDefault("defaultRelationshipType","RELATED").toString();
    }

    public boolean readLabels() {
        return toBoolean(config.getOrDefault("readLabels",false));
    }

    public boolean storeNodeIds() {
        return toBoolean(config.getOrDefault("storeNodeIds", false));
    }

    public boolean separateFiles() {
        return toBoolean(config.getOrDefault("separateFiles", false));
    }

    private ExportFormat format(Object format) {
        return format != null && format instanceof String ? ExportFormat.fromString((String)format) : ExportFormat.NEO4J_SHELL;
    }

    public boolean streamStatements() {
        return streamStatements;
    }

    public boolean writeNodeProperties() {
        return writeNodeProperties;
    }

    public long getTimeoutSeconds() {
        return Util.toLong(config.getOrDefault("timeoutSeconds",100));
    }

    public Map<String, Object> getOptimizations() {
        return optimizations;
    }

    public OptimizationType getOptimizationType() {
        return optimizationType;
    }

}
