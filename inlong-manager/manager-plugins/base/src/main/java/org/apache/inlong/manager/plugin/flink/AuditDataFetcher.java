package org.apache.inlong.manager.plugin.flink;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.boot.configurationprocessor.json.JSONObject;

public class AuditDataFetcher {

    private final String baseUrl;

    public AuditDataFetcher(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public long fetchDataVolumeLastHour() throws Exception {
        String url = baseUrl + "/api/path/to/hourly/data";  // Adjust path as needed
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject json = new JSONObject(responseBody);
                return json.getLong("dataVolume");  // Ensure the JSON key matches what API returns
            }
        }
    }

    public static void main(String[] args) {
        AuditDataFetcher auditDataFetcher = new AuditDataFetcher("asdf");
        try {
            long dataVolume = auditDataFetcher.fetchDataVolumeLastHour();
            System.out.println("Data volume: " + dataVolume);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
