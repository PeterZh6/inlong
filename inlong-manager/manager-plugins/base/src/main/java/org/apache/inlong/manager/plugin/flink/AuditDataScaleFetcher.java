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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.audit.AuditIdEnum;
import org.apache.inlong.manager.plugin.flink.enums.Constants;
import org.apache.inlong.manager.pojo.audit.AuditInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringJoiner;

import static org.apache.inlong.manager.plugin.flink.enums.Constants.DEFAULT_API_MINUTES_PATH;
import static org.apache.inlong.manager.plugin.flink.enums.Constants.DEFAULT_FLOWTYPE;

;

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
    public static long getDataScaleOnMinutesScale(AuditDataScaleRequest2 request) throws Exception {
        int auditId = AuditIdEnum.getAuditId(request.getAuditType(), DEFAULT_FLOWTYPE).getValue();
        StringJoiner urlParameters =
                new StringJoiner("&").add(Constants.PARAMS_START_TIME + "=" + request.getStartTime())
                        .add(Constants.PARAMS_END_TIME + "=" + request.getEndTime())
                        .add(Constants.PARAMS_INLONG_GROUP_ID + "=" + request.getInlongGroupId())
                        .add(Constants.PARAMS_INLONG_STREAM_ID + "=" + request.getInlongStreamId())
                        .add(Constants.PARAMS_AUDIT_ID + "=" + auditId)
                        .add(Constants.PARAMS_AUDIT_CYCLE + "=" + request.getAuditCycle());

        String url = TEST_AUDIT_API_URL + DEFAULT_API_MINUTES_PATH + "?" + urlParameters;
        long count = getCountFromAuditInfo(url);
        long finalCount = count == -1 ? 0 : count;

        return finalCount;
    }



    /**
     * Request audit data from inlong audit, parse the response and return the count.
     *
     * @param url
     * @return
     */
    private static long getCountFromAuditInfo(String url) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String responseString = EntityUtils.toString(entity);
                    LOG.info("Response: {}", responseString);
                    Gson gson = new Gson();
                    JsonObject jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
                    AuditInfo[] auditDataArray = gson.fromJson(jsonObject.getAsJsonArray("data"), AuditInfo[].class);

                    AuditInfo closestAuditInfo = findClosestToNow(auditDataArray);
                    if (closestAuditInfo != null) {
                        LOG.info("Closest AuditEntity Count: {}", closestAuditInfo.getCount());
                        LOG.info("Closest AuditEntity Size: {}", closestAuditInfo.getSize());
                        return closestAuditInfo.getCount();
                    } else {
                        LOG.info("No AuditEntity found.");
                        return -1;
                    }
                }
            } catch (Exception e) {
                LOG.error("Error executing request", e);
            }
        } catch (Exception e) {
            LOG.error("Error creating HTTP client", e);
        }
        return -1;
    }

    /**
     * Find the AuditInfo closest to the current time.
     *
     * @param auditDataArray
     * @return
     */
    private static AuditInfo findClosestToNow(AuditInfo[] auditDataArray) {
        AuditInfo closest = null;
        long closestTimeDiff = Long.MAX_VALUE;

        Date now = new Date();
        for (AuditInfo auditData : auditDataArray) {
            try {
                Date logDate = getLogTsAsDate(auditData.getLogTs());
                long timeDiff = Math.abs(now.getTime() - logDate.getTime());
                if (timeDiff < closestTimeDiff) {
                    closestTimeDiff = timeDiff;
                    closest = auditData;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return closest;
    }

    /**
     * only fetch the latest incoming sort data
     *
     * @param logTs
     * @return
     * @throws ParseException
     */
    private static Date getLogTsAsDate(String logTs) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return formatter.parse(logTs);
    }

    public static void main(String[] args) throws Exception {
        AuditDataScaleFetcher auditDataScaleFetcher = new AuditDataScaleFetcher();
        String auditTypeStr = "CLICKHOUSE";
        String startTime = "2024-07-01T00:00:00";
        String endTime = "2024-07-07T00:10:00";
        String inlongGroupId = "test_pulsar_group";
        String inlongStreamId = "test_pulsar_stream";
        String auditCycle = "1";
        AuditDataScaleRequest2 request = new AuditDataScaleRequest2();
        request.setStartTime(startTime);
        request.setEndTime(endTime);
        request.setInlongGroupId(inlongGroupId);
        request.setInlongStreamId(inlongStreamId);
        request.setAuditCycle(auditCycle);
        request.setAuditType(auditTypeStr);


        try {
            long dataVolume = getDataScaleOnMinutesScale(request);
            LOG.info("Data Volume: {}", dataVolume);
        } catch (Exception e) {
            LOG.error("Error calculating parallelism", e);
            e.printStackTrace();
        }
    }

}



//    /**
//     * get AuditId in order to query Audit API
//     * @param
//     * @return
//     */
//    private String buildSuccessfulAuditId() {
//        String auditType = "DATA_PROXY";
//        FlowType flowType = FlowType.OUTPUT;
//        String auditPreFix = buildSuccessAndFailureFlag(DEFAULT_SUCCESS) + buildRealtimeFlag(DEFAULT_IS_REAL_TIME)
//                + buildDiscardFlag(DEFAULT_DISCARD) + buildRetryFlag(DEFAULT_RETRY);
////        AuditIdEnum baseAuditId = AuditIdEnum.getAuditId(auditType, flowType);
//        int baseAuditId = 6;
////        int auditId = Integer.parseInt(auditPreFix + buildAuditIdSuffix(baseAuditId.getValue()), 2);
//        int auditId = Integer.parseInt(auditPreFix + buildAuditIdSuffix(baseAuditId), 2);
//        return String.valueOf(auditId);
//    }

//    public static String buildSuccessAndFailureFlag(boolean success) {
//        return success ? "0" : "1";
//    }
//
//    public static String buildRealtimeFlag(boolean isRealtime) {
//        return isRealtime ? "0" : "1";
//    }
//
//    public static String buildDiscardFlag(boolean discard) {
//        return discard ? "1" : "0";
//    }
//
//    public static String buildRetryFlag(boolean retry) {
//        return retry ? "1" : "0";
//    }
//
//    public static String buildAuditIdSuffix(int auditId) {
//        StringBuilder auditIdBinaryString = new StringBuilder(Integer.toBinaryString(auditId));
//        for (int i = auditIdBinaryString.length(); i < AUDIT_SUFFIX_LENGTH; i++) {
//            auditIdBinaryString.insert(0, "0");
//        }
//        return auditIdBinaryString.toString();
//    }
