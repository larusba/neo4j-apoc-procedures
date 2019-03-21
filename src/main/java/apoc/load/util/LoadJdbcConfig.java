package apoc.load.util;

import apoc.security.credentials.Credential;
import org.apache.commons.lang.StringUtils;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author ab-Larus
 * @since 03-10-18
 */
public class LoadJdbcConfig {

    private ZoneId zoneId = null;

    private Credentials credentials;

    public LoadJdbcConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        try {
            this.zoneId = config.containsKey("timezone") ?
                    ZoneId.of(config.get("timezone").toString()) : null;
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format("The timezone field contains an error: %s", e.getMessage()));
        }
        this.credentials = config.containsKey("credentials") ? createCredentials((Map<String, String>) config.get("credentials")) : null;
    }

    public ZoneId getZoneId(){
        return this.zoneId;
    }

    public Credentials getCredentials() {
        return this.credentials;
    }

    public static Credentials createCredentials(Map<String,String> credentials) {
        if (!credentials.getOrDefault("user", StringUtils.EMPTY).equals(StringUtils.EMPTY) && !credentials.getOrDefault("password", StringUtils.EMPTY).equals(StringUtils.EMPTY)) {
            return new Credentials(credentials.get("user"), credentials.get("password"));
        } else {
            if(!credentials.getOrDefault("context", StringUtils.EMPTY).equals(StringUtils.EMPTY)){
                String credentialContext = credentials.get("context");

                Map<String, Map<String, Object>> credential = Credential.CREDENTIAL_STORAGE.readData().get(Credential.CREDENTIALS);

                Map<String, Object> usernamePassword = credential.get(credentialContext);

                if(usernamePassword != null && !usernamePassword.isEmpty()) {

                    return new Credentials(credential.get(credentialContext).get("username").toString(), credential.get(credentialContext).get("password").toString());
                }
                throw new IllegalArgumentException("Cannot find credential from context " + credentialContext);
            }

            throw new IllegalArgumentException("In config param credentials must be passed both user and password.");
        }
    }

    public static class Credentials {
        private String user;

        private String password;

        public Credentials(String user, String password){
            this.user = user;

            this.password = password;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }
    }

    public boolean hasCredentials() {
        return this.credentials != null;
    }

}