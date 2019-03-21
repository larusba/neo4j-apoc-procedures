package apoc.security.credentials;

import apoc.Description;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphPropertiesProxy;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static apoc.util.Util.map;

public class Credential {

    public static final String APOC_SECURITY_CREDENTIALS = "apoc.security.credentials";
    public static final String CREDENTIALS = "credentials";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    public static CredentialStorage CREDENTIAL_STORAGE = null;

    @Context
    public Log log;

    //FIXME: manage password visibility
    @Procedure
    @Description("apoc.security.credentials.list - list all credentials")
    public Stream<CredentialInfo> list() {

        List<CredentialInfo> credentials = new ArrayList<>();
        CREDENTIAL_STORAGE.readData().get(CREDENTIALS).forEach((key, value) -> credentials.add(new CredentialInfo(key, value.get(USERNAME).toString(), "********")));

        return credentials.stream();
    }

    //FIXME: manage password visibility
    @Procedure
    @Description("apoc.security.credentials.get(key) - list specific credential")
    public Stream<CredentialInfo> get(@Name("key") String key) {

        Stream<CredentialInfo> credentialInfoStream;
        Map<String, Object> credential = CREDENTIAL_STORAGE.readData().get(CREDENTIALS).get(key);

        if(credential != null){
            credentialInfoStream = Stream.of(new CredentialInfo(key, credential.get(USERNAME).toString(), "********"));
        } else{
            credentialInfoStream = Stream.empty();
        }

        return credentialInfoStream;
    }

    //FIXME: manage password visibility
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.security.credentials.set(key, username, password) - add credential")
    public Stream<CredentialInfo> set(@Name("key")String key, @Name("username")String username, @Name("password")String password) {

        if(StringUtils.isEmpty(key) || StringUtils.isEmpty(username) || StringUtils.isEmpty(password)){
            throw new IllegalArgumentException("In config param key and credentials must be not null or empty.");
        }

        CREDENTIAL_STORAGE.updateCredential(key, map(USERNAME, username, PASSWORD, password));

        return Stream.of(new CredentialInfo(key, username, "********"));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.security.credentials.remove(key) - remove credential")
    public Stream<CredentialInfo> remove(@Name("key")String key) {
        CREDENTIAL_STORAGE.updateCredential(key, null);

        return Stream.empty();
    }

    public static class CredentialStorage {

        private final GraphPropertiesProxy proxy;

        private static CredentialStorage INSTANCE;

        private CredentialStorage(GraphDatabaseAPI api) {
            this.proxy = api.getDependencyResolver().resolveDependency(EmbeddedProxySPI.class).newGraphPropertiesProxy();
        }

        public static CredentialStorage getInstance(GraphDatabaseAPI api) {
            if(INSTANCE == null){
                INSTANCE = new CredentialStorage(api);
            }

            return INSTANCE;
        }

        public static void destroy(){
            INSTANCE = null;
        }

        public Map<String, Map<String, Map<String, Object>>> readData() {
            try (Transaction tx = INSTANCE.proxy.getGraphDatabase().beginTx()) {
                String procedurePropertyData = (String) INSTANCE.proxy.getProperty(APOC_SECURITY_CREDENTIALS, "{\"credentials\":{}}");
                Map result = Util.fromJson(procedurePropertyData, Map.class);
                tx.success();
                return result;
            }
        }

        public synchronized Map<String, Object> updateCredential(String key, Map<String, Object> dataValues) {
            if (key == null) return null;
            try (Transaction tx = INSTANCE.proxy.getGraphDatabase().beginTx()) {
                Map<String, Map<String, Map<String, Object>>> data = readData();
                Map<String, Map<String, Object>> credentialData = data.get(CREDENTIALS);
                Map<String, Object> previous = (dataValues == null) ? credentialData.remove(key) : credentialData.put(key, dataValues);
                if (dataValues != null || previous != null) {
                    INSTANCE.proxy.setProperty(APOC_SECURITY_CREDENTIALS, Util.toJson(data));
                }
                tx.success();
                return previous;
            }
        }
    }

    public static class CredentialInfo {
        public String key;
        public String name;
        public String password;


        CredentialInfo(String key, String name, String password){
            this.key = key;
            this.name = name;
            this.password = password;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CredentialInfo)) return false;
            CredentialInfo that = (CredentialInfo) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

}
