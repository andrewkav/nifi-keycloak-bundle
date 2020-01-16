package com.allstar.nifi.processor.keycloak;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.net.ssl.*;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SupportsBatching
@Tags({"Keycloak", "Get", "Admin"})
@CapabilityDescription("Fetches users from Keycloak server")
public class GetKeycloakAdminUsers extends AbstractProcessor {

    final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed and had any data enriched are routed here")
            .build();

    final static PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("Base URL")
            .addValidator(StandardValidators.URI_VALIDATOR)
            .defaultValue("http://localhost:8080")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    final static PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("Admin user name")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    final static PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Admin password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .required(true)
            .build();

    final static PropertyDescriptor REALM = new PropertyDescriptor.Builder()
            .name("Realm")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    final static PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("Page size")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("200")
            .build();


    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();
    private final ObjectMapper om = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(URL, USER, PASSWORD, REALM, PAGE_SIZE);
    }

    @OnScheduled
    public void setUpClient(final ProcessContext context) throws IOException, UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        okHttpClientAtomicReference.set(null);
        // Create a trust manager that does not validate certificate chains
        final TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                    }

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[]{};
                    }
                }
        };

        // Install the all-trusting trust manager
        final SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        // Create an ssl socket factory with our all-trusting manager
        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0])
                .hostnameVerifier(new HostnameVerifier() {
                    @Override
                    public boolean verify(String hostname, SSLSession session) {
                        return true;
                    }
                });


        okHttpClientAtomicReference.set(builder.build());
    }

    public static final class TokenResponse {
        private String accessToken;

        @JsonProperty("access_token")
        public String getAccessToken() {
            return accessToken;
        }

        @JsonProperty("access_token")
        public void setAccessToken(String accessToken) {
            this.accessToken = accessToken;
        }

    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final OkHttpClient okHttpClient = okHttpClientAtomicReference.get();

        final String baseUrl = context.getProperty(URL).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(USER).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        final String realm = context.getProperty(REALM).evaluateAttributeExpressions().getValue();
        final int pageSize = context.getProperty(PAGE_SIZE).asInteger();

        RequestBody getTokenRequestBody = new FormBody.Builder()
                .add("username", user)
                .add("password", password)
                .add("grant_type", "password")
                .add("client_id", "admin-cli")
                .build();
        Request getTokenRequest = new Request.Builder()
                .url(baseUrl + "/auth/realms/master/protocol/openid-connect/token")
                .post(getTokenRequestBody)
                .build();
        TokenResponse tokenResp = null;
        try (Response responseHttp = okHttpClient.newCall(getTokenRequest).execute()) {
            tokenResp = om.readValue(responseHttp.body().bytes(), TokenResponse.class);
        } catch (IOException e) {
            getLogger().error("unable to get token", e);
            throw new RuntimeException(e);
        }

        if (tokenResp.accessToken == null) {
            throw new RuntimeException("Keycloak admin token must not be null.");
        }

        boolean hasMore = true;
        int first = 0;

        while (hasMore) {
            Request getUsersRequest = new Request.Builder()
                    .url(String.format("%s/auth/admin/realms/%s/users?first=%d&max=%d", baseUrl, realm, first, pageSize))
                    .addHeader("Authorization", "Bearer " + tokenResp.accessToken)
                    .build();

            try (Response responseHttp = okHttpClient.newCall(getUsersRequest).execute()) {
                final byte[] responseBytes = responseHttp.body().bytes();

                List<UserRepresentation> users = om.readValue(responseBytes, List.class);
                hasMore = users.size() != 0;
                first += pageSize;

                if (hasMore) {
                    FlowFile flowFile = session.create();
                    flowFile = session.write(flowFile, out -> out.write(responseBytes));

                    flowFile = session.putAttribute(flowFile, "content-type", "application/json");
                    session.transfer(flowFile, REL_SUCCESS);
                    session.commit();
                }
            } catch (IOException e) {
                getLogger().error("unable to get users", e);
                throw new RuntimeException(e);
            }
        }
    }
}
