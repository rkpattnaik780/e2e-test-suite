package io.managed.services.test.client.openshiftapiadminendpoint;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.managed.services.test.Environment;
import io.managed.services.test.client.exception.ApiUnknownException;
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
import java.io.IOException;

@Log4j2
public class AdminEndpointApi {
    private final RedhatAuthApi redhatAuthApi = new RedhatAuthApi();
    private final OkHttpClient okHttpClient = new OkHttpClient();

    private String obtainRedhatAuthToken() throws IOException, ApiUnknownException {
        return redhatAuthApi.getAdminAuthToken();
    }

    public void patchKafkaInstanceSuspension(String kafkaInstanceId, boolean suspended) throws IOException, ApiUnknownException {

        // request body with suspension content
        JsonObject json = new JsonObject()
            .put("suspended", suspended);
        final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody requestBody = RequestBody.create(JSON, json.toString());

        final String API_HOST = Environment.OPENSHIFT_API_URI;
        //String API_HOST="https://api.stage.openshift.com";

        //request fresh token
        String token = this.redhatAuthApi.getAdminAuthToken();

        Request suspensionRequest = new Request.Builder()
            .url( String.format("%s/api/kafkas_mgmt/v1/admin/kafkas/%s", Environment.OPENSHIFT_API_URI, kafkaInstanceId))
            .addHeader("Content-Type", "application/json")
            .addHeader("Authorization", String.format("Bearer %s", token))
            .patch(requestBody)
            .build();

        Response response = okHttpClient.newCall(suspensionRequest).execute();
        String responseBodyString = response.body().string();
    }

}


class RedhatAuthApi {
    private final OkHttpClient okHttpClient = new OkHttpClient();

    public String getAdminAuthToken() throws IOException, ApiUnknownException {

        // post request credentials
        String credential = Credentials.basic(Environment.STAGE_DATA_PLANE_ADMIN_CLIENT_ID, Environment.STAGE_DATA_PLANE_ADMIN_CLIENT_SECRET);

        // post request body
        RequestBody formBody = new FormBody.Builder()
            .add("grant_type", "client_credentials")
            .build();

        // build request to obtain token which will be used to communicate with Kafka cluster admin endpoint
        Request request = new Request.Builder()
            .url("https://auth.redhat.com/auth/realms/EmployeeIDP/protocol/openid-connect/token")
            .addHeader("Content-Type", "application/x-www-form-urlencoded")
            .addHeader("Authorization", credential)
            .post(formBody)
            .build();

        // send request to obtain token
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();

        if (response.code() != 200) {
            throw new ApiUnknownException(
                response.message(),
                response.code(),
                response.headers().toMultimap(),
                response.body().toString(),
                new Exception());
        }

        // parse the token from response
        String responseBodyString = response.body().string();
        ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        JsonNode jsonNode = objectMapper.readTree(responseBodyString);
        return jsonNode.get("access_token").asText();

    }


}
