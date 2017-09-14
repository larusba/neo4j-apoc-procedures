package apoc.util;


import apoc.ApocConfiguration;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author AgileLARUS
 * @since 03.09.17
 */
public class UriResolver {

    private String user;
    private String password;
    private URI uri;
    private String url;
    private String prefix;
    private AuthToken token;

    public UriResolver(String url, String prefix) throws URISyntaxException {
        this.url = url;
        this.prefix = prefix;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public URI getConfiguredUri() {
        return uri;
    }

    public AuthToken getToken() {
        return token;
    }

    private String getConfiguredUri(String key) {
        String keyUrl = this.prefix + "." + key + ".url";
        if (ApocConfiguration.isEnabled("bolt.url"))
            key = ApocConfiguration.get("bolt.url", key);
        else if (ApocConfiguration.isEnabled(keyUrl))
            key = ApocConfiguration.get(keyUrl, key);
        return key;
    }

    public void initialize() throws URISyntaxException {
        this.url = getConfiguredUri(this.url);
        URI uri;
        try {
            uri = new URI(this.url);
        } catch (URISyntaxException e) {
            throw new URISyntaxException(e.getInput(), e.getMessage());
        }
        this.uri = uri;
        String[] userInfoArray = uri.getUserInfo().split(":");
        this.user = userInfoArray[0];
        this.password = userInfoArray[1];
        if(this.user != null && this.password == null || this.user == null && this.password != null)
            throw new RuntimeException("user and password don't defined check your URL or if you use a key the property in your neo4j.conf file");

        this.token = (this.user != null && this.password != null) ? AuthTokens.basic(this.user, this.password) : AuthTokens.none();
    }
}
