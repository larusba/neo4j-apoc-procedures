package apoc.load.util;

import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

/**
 * @author ab-Larus
 * @since 03-10-18
 */
public class LoadJdbcConfig {

    private ZoneId zoneId;
    private boolean localDateTime;
    private boolean localDate;
    private boolean zoneDateTime;
    private boolean offSetTime;

    public LoadJdbcConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.zoneId = (ZoneId) config.getOrDefault("timezone",ZoneId.of("UTC"));
        this.localDateTime = toBoolean(config.getOrDefault("localDateTime",false));
        this.localDate = toBoolean(config.getOrDefault("localDate",false));
        this.zoneDateTime = toBoolean(config.getOrDefault("zoneDateTime",false));
        this.offSetTime = toBoolean(config.getOrDefault("offSetTime",false));
    }

    public ZoneId zoneId(){
        return this.zoneId;
    }

    public boolean localDateTime() {
        return localDateTime;
    }

    public boolean localDate() {
        return localDate;
    }

    public boolean zoneDateTime() {
        return zoneDateTime;
    }

    public boolean offSetTime() {
        return offSetTime;
    }

}
