package org.apache.inlong.manager.plugin.flink;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.audit.config.OpenApiConstants;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class InLongAuditClient {

    private static final CloseableHttpClient httpClient = HttpClients.createDefault();

    @Value("${audit.query.url:http://127.0.0.1:10080}")
    private String auditQueryUrl;

    public long queryDataVolumeLastHour() throws Exception {
        String url = auditQueryUrl + OpenApiConstants.DEFAULT_API_HOUR_PATH;
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = httpClient.execute(httpGet);
        try {
            String responseBody = EntityUtils.toString(response.getEntity());
            JSONObject json = new JSONObject(responseBody);
            return json.getLong("dataVolume");
        } finally {
            response.close();
        }
    }
}