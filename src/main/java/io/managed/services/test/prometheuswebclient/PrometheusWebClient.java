package io.managed.services.test.prometheuswebclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.UriBuilder;
import lombok.extern.log4j.Log4j2;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Log4j2
public class PrometheusWebClient {

    // TODO url of prometheus (can be obtained with oc client as route and passed as env) - another env, + more dynamic code if desired.
    private String baseUrl;
    private String urlResourcePath;
    // map of key:value header records
    private Map<String, String> headersMap;
    private OkHttpClient httpClient;

    // constructor for builder
    public PrometheusWebClient(String baseUrl, String urlResourcePath, Map<String, String> headersMap, OkHttpClient httpClient) {
        this.baseUrl = baseUrl;
        this.urlResourcePath = urlResourcePath;
        this.headersMap = headersMap;
        this.httpClient = httpClient;
    }

    public QueryResult query(Query query) throws PrometheusException {
        return query(query.toString());
    }

    public QueryResult query(String query) throws PrometheusException {

        var builder = UriBuilder.fromPath(baseUrl);
        builder.path(urlResourcePath);

        try {
            builder.queryParam("query", URLEncoder.encode(query, "ISO-8859-1"));
        } catch (UnsupportedEncodingException ex) {
            throw new PrometheusException(ex);
        }

        var uri = builder.build();
        Request request;
        try {
            var requestBuilder = new Request
                    .Builder()
                    .url(uri.toURL());
            // add all provided headers
            for (Map.Entry<String, String> entry : headersMap.entrySet()) {
                requestBuilder.addHeader(entry.getKey(), entry.getValue());
            }
            request = requestBuilder.build();

        } catch (MalformedURLException ex) {
            throw new PrometheusException(ex);
        }


        try (var response = httpClient.newCall(request).execute()) {
            // all successful requests to Observatorium return 200
            if (response.code() != 200) {
                throw new PrometheusException(String.format("expected 200 but got %d", response.code()));
            }

            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response.body().string(), QueryResult.class);
        } catch (IOException ex) {
            throw new PrometheusException(ex);
        }
    }

    public static class Snapshot {
        Double observedValue;
        Query query;

        public Snapshot(Query query) {
            this.query = query;
        }

        public void setObservedValue(Double observedValue) {
            this.observedValue = observedValue;
        }

        public Double getObservedValue() {
            return observedValue;
        }

        public Query getQuery() {
            return query;
        }
    }


    public static class Query {
        private String metric;

        private List labels = new ArrayList<>();

        private Queue<String> aggregateFunctions = new LinkedList<>();

        public Query metric(String metric) {
            this.metric = metric;
            return this;
        }

        public Query label(String label, String value) {
            labels.add(String.format("%s='%s'", label, value));
            return this;
        }

        public Query aggregateFunction(String functionName) {
            aggregateFunctions.add(functionName);
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            // call all aggregation functions in order they were called in
            for (String elem : aggregateFunctions) {
                sb.append(String.format("%s(", elem));
            }
            sb.append(metric);
            sb.append("{");
            sb.append(String.join(",", labels));
            sb.append("}");
            // append ")" to mark end of function in promql
            sb.append(")".repeat(aggregateFunctions.size()));

            return sb.toString();
        }
    }
}
