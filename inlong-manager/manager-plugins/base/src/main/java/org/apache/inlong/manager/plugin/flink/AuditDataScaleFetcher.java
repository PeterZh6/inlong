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

import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.audit.AuditIdEnum;
import org.apache.inlong.audit.entity.FlowType;
import org.apache.inlong.audit.protocol.AuditData;
import org.apache.inlong.manager.plugin.flink.enums.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringJoiner;

import static org.apache.inlong.manager.plugin.flink.enums.Constants.*;

/**
 * Audit Data Fetcher
 * get data scale from audit api
 */
public class AuditDataScaleFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(AuditDataScaleFetcher.class);

    public AuditDataScaleFetcher() {
    }

    // TODO read audit api url from config
    private static final String TEST_AUDIT_API_URL = "http://172.28.128.1:10080";

    /**
     * Get data scale on minutes scale
     *
     * @param request
     * @return
     * @throws Exception
     */
    public int getDataScaleOnMinutesScale(AuditDataScaleRequest2 request) throws Exception {
        StringJoiner urlParameters = new StringJoiner("&").add(Constants.PARAMS_START_TIME + "=" + request.getStartTime()).add(Constants.PARAMS_END_TIME + "=" + request.getEndTime()).add(Constants.PARAMS_INLONG_GROUP_ID + "=" + request.getInlongGroupId()).add(Constants.PARAMS_INLONG_STREAM_ID + "=" + request.getInlongStreamId()).add(Constants.PARAMS_AUDIT_ID + "=" + request.getAuditId()).add(Constants.PARAMS_AUDIT_CYCLE + "=" + request.getAuditCycle());

        String url = TEST_AUDIT_API_URL + DEFAULT_API_MINUTES_PATH + "?" + urlParameters;

        CloseableHttpResponse response;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {


            HttpGet httpGet = new HttpGet(url);
            response = httpClient.execute(httpGet);
        }


        String responseString = EntityUtils.toString(response.getEntity());
        //parse JSON response
        Gson gson = new Gson();
        AuditData[] auditDataArray = gson.fromJson(responseString, AuditData[].class);

        int totalCount = 0;
        for (AuditData auditData : auditDataArray) {
            totalCount += auditData.getCount();
        }

        return totalCount;
    }

    /**
     * get AuditId in order to query Audit API
     * @param
     * @return
     */
    private String getAuditID() {
        String auditType = "DATA_PROXY";
        FlowType flowType = FlowType.OUTPUT;
        String auditPreFix = buildSuccessAndFailureFlag(DEFAULT_SUCCESS) + buildRealtimeFlag(DEFAULT_IS_REAL_TIME) + buildDiscardFlag(DEFAULT_DISCARD) + buildRetryFlag(DEFAULT_RETRY);
        AuditIdEnum baseAuditId = AuditIdEnum.getAuditId(auditType, flowType);
        int auditId = Integer.parseInt(auditPreFix + buildAuditIdSuffix(baseAuditId.getValue()), 2);
        return String.valueOf(auditId);
    }

    private static String  buildSuccessAndFailureFlag(boolean success) {return success ? "0" : "1";}

    private static String buildRealtimeFlag(boolean isRealtime) {return isRealtime ? "0" : "1";}

    private static String buildDiscardFlag(boolean discard) {
        return discard ? "1" : "0";
    }

    private static String buildRetryFlag(boolean retry) {
        return retry ? "1" : "0";
    }

    private static String buildAuditIdSuffix(int auditId) {
        StringBuilder auditIdBinaryString = new StringBuilder(Integer.toBinaryString(auditId));
        for (int i = auditIdBinaryString.length(); i < AUDIT_SUFFIX_LENGTH; i++) {
            auditIdBinaryString.insert(0, "0");
        }
        return auditIdBinaryString.toString();
    }

    public static void main(String[] args) {
        AuditDataScaleFetcher auditDataScaleFetcher = new AuditDataScaleFetcher();
        String startTime = "2024-07-01T00:00:00";
        String endTime = "2024-07-07T00:10:00";
        String inlongGroupId = "test_pulsar_group";
        String inlongStreamId = "test_pulsar_stream";
        String auditId = "1073741825";
        String auditCycle = "1";
        AuditDataScaleRequest2 request = new AuditDataScaleRequest2(startTime, endTime, inlongGroupId, inlongStreamId, auditId, auditCycle);
        try {
            long dataVolume = auditDataScaleFetcher.getDataScaleOnMinutesScale(request);
            LOG.info("Data Volume: {}", dataVolume);
        } catch (Exception e) {
            LOG.error("Error calculating parallelism", e);
            e.printStackTrace();
        }
    }

}
