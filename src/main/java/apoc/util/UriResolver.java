package apoc.util;


import apoc.ApocConfiguration;

import java.net.URI;
import java.net.URISyntaxException;
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

    public UriResolver(String url, Map<String, Object> config) throws URISyntaxException {
        url = ApocConfiguration.get("apoc.bolt.url", url);
        System.out.println("trigger " + ApocConfiguration.get("apoc.trigger.enabled"));
        System.out.println("url = " + url);
        System.out.println("ApocConfiguration.get(\"apoc.bolt.url\", url) = " + ApocConfiguration.get("apoc.bolt.url", url));
        URI uri = new URI(url);
        if(uri.getUserInfo() != null) {
            String[] userInfoArray = uri.getUserInfo().split(":");
            this.user = userInfoArray[0];
            this.password = userInfoArray[1];
        }
        this.context = uri.getPath();
        this.urlDriver = uri.getScheme() + "://" + uri.getHost();
        this.port = uri.getPort();
        this.query = uri.getQuery();
    }

    public String getUser() {return user; }

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
}
