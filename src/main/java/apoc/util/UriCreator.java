package apoc.util;


import java.net.URI;
import java.util.Map;

/**
 * @author AgileLARUS
 * @since 03.09.17
 */
public class UriCreator {

    private String url;
    private Map<String, Object> config;
    private String user;
    private String password;
    private String urlDriver;

    public UriCreator(String url, Map<String, Object> config) {
        this.url = url;
        this.config = config;
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

    public UriCreator invoke() {
        URI uri = URI.create(url);

        String[] userInfoArray = uri.getUserInfo().split(":");
        user = userInfoArray[0];
        password = userInfoArray[1];
        urlDriver = uri.getScheme() + "://" + uri.getHost();
        return this;
    }
}
