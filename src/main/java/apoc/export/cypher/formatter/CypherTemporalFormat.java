package apoc.export.cypher.formatter;

import org.neo4j.values.storable.DurationValue;

import java.time.*;

public enum CypherTemporalFormat {

    DATE("date"),
    TIME("time"),
    LOCAL_TIME("localtime"),
    DATE_TIME("datetime"),
    LOCAL_DATE_TIME("localdatetime"),
    DURATION("duration");

    private static final String TEMPLATE = "%s('%s')";

    private String value;

    CypherTemporalFormat(String value) {
        this.value = value;
    }

    public static String format(Object value) {
        if(value == null) return null;
        return String.format(TEMPLATE, of(value).value, value.toString());
    }

    public static CypherTemporalFormat of(Object value) {

        if (value == null) {
            return null;
        } else if (value instanceof ZonedDateTime) {
            return DATE_TIME;
        } else if (value instanceof OffsetTime) {
            return TIME;
        } else if (value instanceof LocalDate) {
            return DATE;
        } else if (value instanceof LocalDateTime) {
            return LOCAL_DATE_TIME;
        } else if (value instanceof LocalTime) {
            return LOCAL_TIME;
        } else if (value instanceof DurationValue) {
            return DURATION;
        } else {
            throw new RuntimeException("Can't format the Temporal/Duration into a cypher statement");
        }

    }
}
