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
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.audit.AuditIdEnum;
import org.apache.inlong.audit.entity.FlowType;
import org.apache.inlong.manager.pojo.audit.AuditInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.StringJoiner;

import static java.lang.Math.ceil;

/**
 *     This class is used to calculate the recommended parallelism based on the maximum message per second per core.
 *     The data volume is calculated based on the average data count per hour.
 *     The data count is retrieved from the inlong audit API.
 */
@Slf4j
@Component
public class FlinkParallelismOptimizer {

    @Value("${audit.query.url:http://127.0.0.1:10080}")
    public String auditQueryUrl;

    private static final long DEFAULT_DATA_VOLUME = 1000;
    private static final long DEFAULT_MAXIMUM_MESSAGE_PER_SECOND_PER_CORE = 1000;
    private static final int MAX_PARALLELISM = 2048;
    private long maximumMessagePerSecondPerCore;
    private static final int DEFAULT_PARALLELISM = 1;
    private static final long DEFAULT_ERROR_DATA_COUNT = 0L;
    private static final String PARAMS_START_TIME = "startTime";
    private static final String PARAMS_END_TIME = "endTime";
    private static final String PARAMS_AUDIT_ID = "auditId";
    private static final String PARAMS_AUDIT_CYCLE = "auditCycle";
    private static final String PARAMS_INLONG_GROUP_ID = "inlongGroupId";
    private static final String PARAMS_INLONG_STREAM_ID = "inlongStreamId";
    private static final String DEFAULT_API_MINUTES_PATH = "/audit/query/minutes";
    private static final FlowType DEFAULT_FLOWTYPE = FlowType.OUTPUT;
    private static final String DEFAULT_AUDIT_TYPE = "DataProxy";
    private static final String AMPERSAND = "&";
    private static final String EQUAL = "=";
    private static final String QUESTION_MARK = "?";
    private static final double SECONDS_PER_HOUR = 3600.0;
    private static final String AUDIT_CYCLE_REALTIME = "1";
    private static final String AUDIT_QUERY_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"; //sample time format: 2024-08-23T22:47:38.866


    /**
     * Calculate recommended parallelism based on maximum message per second per core
     *
     * @return Recommended parallelism
     */
    public int calculateRecommendedParallelism(List<InlongStreamInfo> streamInfos) {
        long dataVolume;
        InlongStreamInfo streamInfo = streamInfos.get(0);
        try {
            dataVolume = getAverageDataVolumePerHour(streamInfo);
            log.info("Retrieved data volume: {}", dataVolume);
        } catch (Exception e) {
            log.error("Error retrieving data volume: {}", e.getMessage(), e);
            log.warn("Using default data volume: {}", DEFAULT_DATA_VOLUME);
            dataVolume = DEFAULT_DATA_VOLUME;
        }
        int newParallelism = (int) ceil((double) dataVolume / maximumMessagePerSecondPerCore);
        newParallelism = Math.max(newParallelism, DEFAULT_PARALLELISM); // Ensure parallelism is at least the default value
        newParallelism = Math.min(newParallelism, MAX_PARALLELISM); // Ensure parallelism is at most MAX_PARALLELISM
        log.info("Calculated parallelism: {} for data volume: {}", newParallelism, dataVolume);
        return newParallelism;
    }

    /**
     * Initialize maximum message per second per core based on configuration
     *
     * @param maximumMessagePerSecondPerCore The maximum messages per second per core
     */
    public void setMaximumMessagePerSecondPerCore(Integer maximumMessagePerSecondPerCore) {
        if (maximumMessagePerSecondPerCore == null || maximumMessagePerSecondPerCore <= 0) {
            this.maximumMessagePerSecondPerCore = DEFAULT_MAXIMUM_MESSAGE_PER_SECOND_PER_CORE;
            log.error("Illegal flink.maxpercore property, must be nonnull and positive, using default value: {}", DEFAULT_MAXIMUM_MESSAGE_PER_SECOND_PER_CORE);
        } else {
            this.maximumMessagePerSecondPerCore = maximumMessagePerSecondPerCore;
        }
    }

    /**
     * Get data scale on a minutes scale
     *
     * @param streamInfo inlong stream info
     * @return The average data count per hour
     */
    private long getAverageDataVolumePerHour(InlongStreamInfo streamInfo) {
        // Since the audit module use local time, we need to use ZonedDateTime to get the current time
        ZonedDateTime endTime = ZonedDateTime.now();
        ZonedDateTime startTime = endTime.minusHours(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(AUDIT_QUERY_DATE_TIME_FORMAT);

        int auditId = AuditIdEnum.getAuditId(DEFAULT_AUDIT_TYPE, DEFAULT_FLOWTYPE).getValue();
        StringJoiner urlParameters = new StringJoiner(AMPERSAND)
                .add(PARAMS_START_TIME + EQUAL + startTime.format(formatter))
                .add(PARAMS_END_TIME + EQUAL + endTime.format(formatter))
                .add(PARAMS_INLONG_GROUP_ID + EQUAL + streamInfo.getInlongGroupId())
                .add(PARAMS_INLONG_STREAM_ID + EQUAL + streamInfo.getInlongStreamId())
                .add(PARAMS_AUDIT_ID + EQUAL + auditId)
                .add(PARAMS_AUDIT_CYCLE + EQUAL + AUDIT_CYCLE_REALTIME);

        String url = auditQueryUrl + DEFAULT_API_MINUTES_PATH + QUESTION_MARK + urlParameters;
        long totalCount = getCountFromAuditInfo(url);

        return totalCount == DEFAULT_ERROR_DATA_COUNT ? DEFAULT_ERROR_DATA_COUNT : (long) (totalCount / SECONDS_PER_HOUR);
    }

    /**
     * Request audit data from inlong audit API, parse the response and return the total count in the given time range.
     *
     * @param url The URL to request data from
     * @return The total count of the audit data
     */
    private long getCountFromAuditInfo(String url) {
        log.debug("Requesting audit data from URL: {}", url);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                return parseResponse(response);
            } catch (IOException e) {
                log.error("Error executing HTTP request to audit API: {}", url, e);
            }
        } catch (IOException e) {
            log.error("Error creating or closing HTTP client: {}", url, e);
        }
        return DEFAULT_ERROR_DATA_COUNT;
    }

    /**
     * Parse the HTTP response and calculate the total count from the audit data.
     *
     * @param response The HTTP response
     * @return The total count of the audit data
     * @throws IOException If an I/O error occurs
     */
    private long parseResponse(CloseableHttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            log.warn("Empty response entity from audit API, returning default count.");
            return DEFAULT_ERROR_DATA_COUNT;
        }

        String responseString = EntityUtils.toString(entity);
        log.info("Response: {}", responseString);

        JsonObject jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
        AuditInfo[] auditDataArray = new Gson().fromJson(jsonObject.getAsJsonArray("data"), AuditInfo[].class);

        long totalCount = 0L;
        for (AuditInfo auditData : auditDataArray) {
            if (auditData != null) {
                log.debug("AuditInfo Count: {}, Size: {}", auditData.getCount(), auditData.getSize());
                totalCount += auditData.getCount();
            } else {
                log.error("Null AuditInfo found in response data.");
            }
        }
        return totalCount;
    }
}
