package apoc.util;


import apoc.ApocConfiguration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

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
    private Map<String, String> queryParams;

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

    public String getUrlDriver() {
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

    public Map<String, String> getQueryParams() {
        return queryParams;
    }

    private String getUri(String key, String prefix) {
        String keyUrl = prefix + "." + key + ".url";
        if (key == null || key.equals(""))
            key = ApocConfiguration.get("bolt.url", key);
        else if (ApocConfiguration.isEnabled(keyUrl))
            key = ApocConfiguration.get(keyUrl, key);

        return key;
    }

    public void initialize() throws URISyntaxException {
        this.url = getUri(this.url, this.prefix);
        URI uri;
        try {
            uri = new URI(this.url);
        } catch (URISyntaxException e) {
            throw new URISyntaxException(e.getInput(), e.getMessage());
        }
        if (uri.getUserInfo() != null) {
            String[] userInfoArray = uri.getUserInfo().split(":");
            this.user = userInfoArray[0];
            this.password = userInfoArray[1];
        } else
            throw new RuntimeException("user and password don't defined check your URL or if you use a key the property in your neo4j.conf file");
        this.context = uri.getPath();
        this.urlDriver = uri.getScheme() + "://" + uri.getHost();
        this.port = uri.getPort();
        this.query = uri.getQuery();
        if (this.query != null)
            this.queryParams = getQueryParams(uri);
    }

    private Map<String, String> getQueryParams(URI uri) {
        Map<String, String> queryParamsMap = new HashMap<>();
        String[] pairs = uri.getQuery().split("&");
        for (String pair : pairs) {
            int index = pair.indexOf("=");
            if (index != -1)
                queryParamsMap.put(pair.substring(0, index), pair.substring(index + 1));
        }
        return queryParamsMap;
    }
}
