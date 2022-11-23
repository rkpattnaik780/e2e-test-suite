package io.managed.services.test.client.openshiftapiadmin;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.managed.services.test.Environment;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import okhttp3.Call;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@Log4j2
public class OpenshiftAdminEndpointApi {

    private final RedhatAuthApi redhatAuthApi;

    private String obtainRedhatAuthToken() throws Exception {
        return redhatAuthApi.getAdminAuthToken();
    }

    public OpenshiftAdminEndpointApi(String rhAuthUsername, String rhAuthPassword) {
        this.redhatAuthApi = new RedhatAuthApi(rhAuthUsername, rhAuthPassword);
    }

    public void patchKafkaInstanceSuspension(String kafkaInstanceId, boolean suspended) throws Exception {

        // request body with suspension content
        JsonObject json = new JsonObject()
            .put("suspended", suspended);
        final MediaType jsonMediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody requestBody = RequestBody.create(jsonMediaType, json.toString());

        log.info("request fresh token to communicate with admin endpoint");
        String token = this.obtainRedhatAuthToken();

        //request url
        String requestUrl = String.format("%s/api/kafkas_mgmt/v1/admin/kafkas/%s", Environment.OPENSHIFT_API_URI, kafkaInstanceId);

        Request suspensionRequest = new Request.Builder()
            .url(requestUrl)
            .addHeader("Content-Type", "application/json")
            .addHeader("Authorization", String.format("Bearer %s", token))
            .patch(requestBody)
            .build();

        Response response = new OkHttpClient().newCall(suspensionRequest).execute();
        if (response.code() != 200)
            throw new Exception(String.format("response code %d while patching kafka at admin endpoint %s", response.code(), requestUrl));
        log.info("kafka instance '{}', patched", kafkaInstanceId);
    }
}

@Log4j2
class RedhatAuthApi {

    private final OkHttpClient okHttpClient = new OkHttpClient();
    private final String credentialsName;
    private final String credentialsPassword;

    public RedhatAuthApi(String credentialsName, String credentialsPassword) {
        this.credentialsName = credentialsName;
        this.credentialsPassword = credentialsPassword;
    }

    public String getAdminAuthToken() throws Exception {

        // request components
        String credential = Credentials.basic(credentialsName, credentialsPassword);
        RequestBody formBody = new FormBody.Builder().add("grant_type", "client_credentials").build();
        String requestUrl = "https://auth.redhat.com/auth/realms/EmployeeIDP/protocol/openid-connect/token";

        // build request to obtain token which will be used to communicate with Kafka cluster admin endpoint
        Request request = new Request.Builder()
            .url(requestUrl)
            .addHeader("Content-Type", "application/x-www-form-urlencoded")
            .addHeader("Authorization", credential)
            .post(formBody)
            .build();

        // send request to obtain token
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();

        if (response.code() != 200)
            throw new Exception(String.format("response code %d while obtaining token from %s", response.code(), requestUrl));

        // parse the token from response
        log.info("token successfully obtained");
        String responseBodyString = response.body().string();
        ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        JsonNode jsonNode = objectMapper.readTree(responseBodyString);
        return jsonNode.get("access_token").asText();
    }
}
