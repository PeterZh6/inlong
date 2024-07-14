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

import lombok.extern.slf4j.Slf4j;

import static java.lang.Math.ceil;

@Slf4j
public class DataScaleMonitor {

    private AuditDataScaleFetcher auditDataScaleFetcher;

    //TODO: leave this to be configurable
    private static final double MAXIMUM_MESSAGE_PER_SECOND_PER_CORE = 1000.0;

    private static final int DEFAULT_PARALLELISM = 1;

    private static final long DEFAULT_DATA_VOLUME = 1000;

    public DataScaleMonitor() {
        this.auditDataScaleFetcher = new AuditDataScaleFetcher();
    }

    /**
     * Retrieve data volume
     * @param request
     * @return data volume, return default data volume if failed
     */
    public long retrieveDataVolume(AuditDataScaleRequest2 request) {

        try {
            long dataVolume = auditDataScaleFetcher.getDataScaleOnMinutesScale(request);
            log.info("retrieved data volume: {}", dataVolume);
            return dataVolume;
        } catch (Exception e) {
            log.error("Error calculating parallelism", e);
            log.error("Using default data volume: {}", DEFAULT_DATA_VOLUME);
        }
        return DEFAULT_DATA_VOLUME;
    }

    /**
     * Calculate recommended parallelism based on the assumption that each core processes 1000 records per second
     *
     * @return recommended parallelism
     */
    private int calculateRecommendedParallelism(AuditDataScaleRequest2 request) {

        long dataVolume = retrieveDataVolume(request);
        int newParallelism = (int) ceil(dataVolume / MAXIMUM_MESSAGE_PER_SECOND_PER_CORE);
        return Math.max(newParallelism, DEFAULT_PARALLELISM); // make sure parallelism is at least 1
    }

    /**
     * Get recommended parallelism
     *
     * @return recommended parallelism
     */
    public int getRecommendedParallelism(AuditDataScaleRequest2 request) {
        return calculateRecommendedParallelism(request);
    }
}
