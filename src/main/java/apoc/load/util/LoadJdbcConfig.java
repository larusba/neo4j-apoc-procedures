package apoc.load.util;

import org.apache.commons.lang.StringUtils;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

/**
 * @author ab-Larus
 * @since 03-10-18
 */
public class LoadJdbcConfig {

    private ZoneId zoneId = null;

    private Map<String,String> credentials;

    public LoadJdbcConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        try {
            this.zoneId = config.containsKey("timezone") ?
                    ZoneId.of(config.get("timezone").toString()) : null;
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format("The timezone field contains an error: %s", e.getMessage()));
        }
        this.credentials = config.containsKey("credentials") ? validateCredentials((Map<String, String>) config.get("credentials")) : Collections.emptyMap();
    }

    public ZoneId getZoneId(){
        return this.zoneId;
    }

    public Map<String,String> getCredentials() {
        return this.credentials;
    }

    private Map<String, String> validateCredentials(Map<String,String> credentials) {
        if (!credentials.getOrDefault("user", StringUtils.EMPTY).equals(StringUtils.EMPTY) && !credentials.getOrDefault("password", StringUtils.EMPTY).equals(StringUtils.EMPTY)) {
            return credentials;
        } else {
            throw new IllegalArgumentException("In config param credentials must be passed both user and password.");
        }
    }

}