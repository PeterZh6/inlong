/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.plugin.flink;

import org.apache.inlong.manager.plugin.flink.enums.Constants;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.StringJoiner;

import static org.apache.inlong.manager.plugin.flink.enums.Constants.DEFAULT_API_MINUTES_PATH;

/**
 *  Audit Data Fetcher
 *  get data scale from audit api
 */
public class AuditDataFetcher {

    public AuditDataFetcher() {
    }

    // TODO read audit api url from config
    private static final String TEST_AUDIT_API_URL = "http://172.28.128.1:10080";

    /**
     * Get data scale on minutes scale
     * @param request
     * @return
     * @throws Exception
     */
    public int getDataScaleOnMinutesScale(AuditDataScaleRequest2 request) throws Exception {
        StringJoiner urlParameters = new StringJoiner("&")
                .add(Constants.PARAMS_START_TIME + "=" + request.getStartTime())
                .add(Constants.PARAMS_END_TIME + "=" + request.getEndTime())
                .add(Constants.PARAMS_INLONG_GROUP_Id + "=" + request.getInlongGroupId())
                .add(Constants.PARAMS_INLONG_STREAM_Id + "=" + request.getInlongStreamId())
                .add(Constants.PARAMS_AUDIT_ID + "=" + request.getAuditId())
                .add(Constants.PARAMS_AUDIT_CYCLE + "=" + request.getAuditCycle());

        URL url = new URL(TEST_AUDIT_API_URL + DEFAULT_API_MINUTES_PATH + "?" + urlParameters);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        System.out.println(response.toString());
        in.close();
        // 解析JSON响应
        Gson gson = new Gson();
        JsonObject jsonResponse = gson.fromJson(response.toString(), JsonObject.class);
        JsonArray data = jsonResponse.getAsJsonArray("data");
        int totalCount = 0;

        for (JsonElement element : data) {
            JsonObject statData = element.getAsJsonObject();
            totalCount += statData.get("count").getAsInt();
        }

        return totalCount;
    }

    public static void main(String[] args) {
        AuditDataFetcher auditDataFetcher = new AuditDataFetcher();
        String startTime = "2024-07-01T00:00:00";
        String endTime = "2024-07-07T00:10:00";
        String inlongGroupId = "test_pulsar_group";
        String inlongStreamId = "test_pulsar_stream";
        String auditId = "1073741825";
        String auditCycle = "1";
        AuditDataScaleRequest2 request =
                new AuditDataScaleRequest2(startTime, endTime, inlongGroupId, inlongStreamId, auditId, auditCycle);
        try {
            long dataVolume = auditDataFetcher.getDataScaleOnMinutesScale(request);
            System.out.println("Data volume: " + dataVolume);
        } catch (Exception e) {
            System.out.println("Error calculating parallelism");
            e.printStackTrace();
        }
    }

}
