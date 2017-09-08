package apoc.util;


import apoc.ApocConfiguration;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author AgileLARUS
 * @since 03.09.17
 */
public class UriResolver {

    private String user;
    private String password;
    private String urlDriver;
    private int port;
    private String context;
    private String query;
    private String url;
    private String prefix;

    public UriResolver(String url, String prefix) throws URISyntaxException {
        this.url = url;
        this.prefix = prefix;
    }

    public void initialize() throws URISyntaxException {
        this.url = getUri(this.url, this.prefix);
        URI uri = new URI(this.url);
        if (uri.getUserInfo() != null) {
            String[] userInfoArray = uri.getUserInfo().split(":");
            this.user = userInfoArray[0];
            this.password = userInfoArray[1];
        }
        this.context = uri.getPath();
        this.urlDriver = uri.getScheme() + "://" + uri.getHost();
        this.port = uri.getPort();
        this.query = uri.getQuery();
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getUriDriver() {
        return urlDriver;
    }

    public int getPort() {
        return port;
    }

    public String getContext() {
        return context;
    }

    public String getQuery() {
        return query;
    }

    private String getUri(String key, String prefix) {
        String keyUrl = prefix + "." + key + ".url";
        if (key == null || key.equals(""))
            key = ApocConfiguration.get("bolt.url", key);
        else if (ApocConfiguration.isEnabled(keyUrl))
            key = ApocConfiguration.get(keyUrl, key);

        return key;
    }
}
