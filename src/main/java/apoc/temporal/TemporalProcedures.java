package apoc.temporal;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;

public class TemporalProcedures
{
    public static final Map<String, DateTimeFormatter> ISO_DATE_FORMAT;

    static {
        Map<String, DateTimeFormatter> map = new HashMap<>();
        //Static default formatters from Java ISO and Elasticsearch patterns
        map.put("basic_iso_date", DateTimeFormatter.BASIC_ISO_DATE);
        map.put("iso_date", DateTimeFormatter.ISO_DATE);
        map.put("iso_instant", DateTimeFormatter.ISO_INSTANT);
        map.put("iso_date_time", DateTimeFormatter.ISO_DATE_TIME);
        map.put("iso_local_date", DateTimeFormatter.ISO_LOCAL_DATE);
        map.put("iso_local_date_time", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        map.put("iso_local_time", DateTimeFormatter.ISO_LOCAL_TIME);
        map.put("iso_offset_date", DateTimeFormatter.ISO_OFFSET_DATE);
        map.put("iso_offset_date_time", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        map.put("iso_offset_time", DateTimeFormatter.ISO_OFFSET_TIME);
        map.put("iso_ordinal_date", DateTimeFormatter.ISO_ORDINAL_DATE);
        map.put("iso_time", DateTimeFormatter.ISO_TIME);
        map.put("iso_week_date", DateTimeFormatter.ISO_WEEK_DATE);
        map.put("iso_zoned_date_time", DateTimeFormatter.ISO_ZONED_DATE_TIME);
        map.put("basic_date", DateTimeFormatter.ofPattern("yyyyMMdd"));
        map.put("basic_date_time", DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSZ"));
        map.put("basic_date_time_no_millis", DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssZ"));
        map.put("basic_ordinal_date", DateTimeFormatter.ofPattern("yyyyDDD"));
        map.put("basic_ordinal_date_time", DateTimeFormatter.ofPattern("yyyyDDD'T'HHmmss.SSSZ"));
        map.put("basic_ordinal_date_time_no_millis", DateTimeFormatter.ofPattern("yyyyDDD'T'HHmmssZ"));
        map.put("basic_time", DateTimeFormatter.ofPattern("HHmmss.SSSZ"));
        map.put("basic_time_no_millis", DateTimeFormatter.ofPattern("HHmmssZ"));
        map.put("basic_t_time", DateTimeFormatter.ofPattern("'T'HHmmss.SSSZ"));
        map.put("basic_t_time_no_millis", DateTimeFormatter.ofPattern("'T'HHmmssZ"));
        map.put("basic_week_date", DateTimeFormatter.ofPattern("xxxx'W'wwe"));
        map.put("basic_week_date_time", DateTimeFormatter.ofPattern("xxxx'W'wwe'T'HHmmss.SSSZ"));
        map.put("basic_week_date_time_no_millis", DateTimeFormatter.ofPattern("xxxx'W'wwe'T'HHmmssZ"));
        map.put("date", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        map.put("date_hour", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH"));
        map.put("date_hour_minute", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"));
        map.put("date_hour_minute_second", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        map.put("date_hour_minute_second_fraction", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        map.put("date_hour_minute_second_millis", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        map.put("date_time", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ"));
        map.put("date_time_no_millis", DateTimeFormatter.ofPattern("yyy-MM-dd'T'HH:mm:ssZZ"));
        map.put("hour", DateTimeFormatter.ofPattern("HH"));
        map.put("hour_minute", DateTimeFormatter.ofPattern("HH:mm"));
        map.put("hour_minute_second", DateTimeFormatter.ofPattern("HH:mm:ss"));
        map.put("hour_minute_second_fraction", DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
        map.put("hour_minute_second_millis", DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
        map.put("ordinal_date", DateTimeFormatter.ofPattern("yyyy-DDD"));
        map.put("ordinal_date_time", DateTimeFormatter.ofPattern("yyyy-DDD'T'HH:mm:ss.SSSZZ"));
        map.put("ordinal_date_time_no_millis", DateTimeFormatter.ofPattern("yyyy-DDD'T'HH:mm:ssZZ"));
        map.put("time", DateTimeFormatter.ofPattern("HH:mm:ss.SSSZZ"));
        map.put("time_no_millis", DateTimeFormatter.ofPattern("HH:mm:ssZZ"));
        map.put("t_time", DateTimeFormatter.ofPattern("'T'HH:mm:ss.SSSZZ"));
        map.put("t_time_no_millis", DateTimeFormatter.ofPattern("'T'HH:mm:ssZZ"));
        map.put("week_date", DateTimeFormatter.ofPattern("xxxx-'W'ww-e"));
        map.put("week_date_time", DateTimeFormatter.ofPattern("xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ"));
        map.put("week_date_time_no_millis", DateTimeFormatter.ofPattern("xxxx-'W'ww-e'T'HH:mm:ssZZ"));
        map.put("weekyear", DateTimeFormatter.ofPattern("xxxx"));
        map.put("weekyear_week", DateTimeFormatter.ofPattern("xxxx-'W'ww"));
        map.put("weekyear_week_day", DateTimeFormatter.ofPattern("xxxx-'W'ww-e"));
        map.put("year", DateTimeFormatter.ofPattern("yyyy"));
        map.put("year_month", DateTimeFormatter.ofPattern("yyyy-MM"));
        map.put("year_month_day", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        ISO_DATE_FORMAT = Collections.unmodifiableMap(map);
    }


    /**
     * Format a temporal value to a String
     *
     * @param input     Any temporal type
     * @param format    A valid DateTime format pattern (ie yyyy-MM-dd'T'HH:mm:ss.SSSS)
     * @return
     */
    @UserFunction( "apoc.temporal.format" )
    @Description( "apoc.temporal.format(input, format) | Format a temporal value" )
    public String format(
            @Name( "temporal" ) Object input,
            @Name( value = "format", defaultValue = "yyyy-MM-dd") String format
    ) {

        try {
            DateTimeFormatter formatter = null;
            if(ISO_DATE_FORMAT.containsKey(format.toLowerCase())) {
                formatter = ISO_DATE_FORMAT.get(format.toLowerCase());
            }else {
                formatter = DateTimeFormatter.ofPattern(format);
            }

            if (input instanceof LocalDate) {
                return ((LocalDate) input).format(formatter);
            } else if (input instanceof ZonedDateTime) {
                return ((ZonedDateTime) input).format(formatter);
            } else if (input instanceof LocalDateTime) {
                return ((LocalDateTime) input).format(formatter);
            } else if (input instanceof LocalTime) {
                return ((LocalTime) input).format(formatter);
            } else if (input instanceof OffsetTime) {
                return ((OffsetTime) input).format(formatter);
            } else if (input instanceof DurationValue) {
                return formatDuration(input, format);
            }
        } catch (Exception e){
            throw new RuntimeException("Available formats are:\n" +
                    ISO_DATE_FORMAT.entrySet().stream().map( entry -> entry.getKey()).collect(Collectors.joining("\n")) +
                    "\nSee also: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping-date-format.html#built-in-date-formats " +
                    "and https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
        }
        return input.toString();
    }

    /**
     * Convert a Duration into a LocalTime and format the value as a String
     *
     * @param input
     * @param format
     * @return
     */
    @UserFunction( "apoc.temporal.formatDuration" )
    @Description( "apoc.temporal.formatDuration(input, format) | Format a Duration" )
    public String formatDuration(
            @Name("input") Object input,
            @Name("format") String format
    ) {
        try {
            LocalDateTime midnight = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0);
            LocalDateTime newDuration = midnight.plus( (DurationValue) input );

            DateTimeFormatter formatter = ISO_DATE_FORMAT.containsKey(format.toLowerCase()) ? ISO_DATE_FORMAT.get(format.toLowerCase()) : DateTimeFormatter.ofPattern(format);

            return newDuration.format(formatter);
        } catch (Exception e){
        throw new RuntimeException("Available formats are:\n" +
                ISO_DATE_FORMAT.entrySet().stream().map( entry -> entry.getKey()).collect(Collectors.joining("\n")) +
                "\nSee also: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping-date-format.html#built-in-date-formats " +
                "and https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
}
    }


}
