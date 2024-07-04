package org.apache.inlong.manager.plugin.flink;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import static java.lang.Math.ceil;

@Slf4j
public class DataScaleMonitor {


    private AuditDataFetcher auditDataFetcher;

    private static final double MAXIMUM_MESSAGE_PER_SECOND_PER_CORE = 1000.0;

    private static final int DEFAULT_PARALLELISM = 1;

    private static final long DEFAULT_DATA_VOLUME = 1000;

    @Value("${audit.query.url:http://127.0.0.1:10080}")
    private String baseUrl;
    public DataScaleMonitor() {
        this.auditDataFetcher = new AuditDataFetcher(baseUrl);
    }

    public long retrieveDataVolume() {
        try {
            long dataVolume = auditDataFetcher.fetchDataVolumeLastHour();
            log.info("retrieved data volume: {}", dataVolume);
            return dataVolume;
        } catch (Exception e) {
            log.error("Error calculating parallelism", e);
        }
        return DEFAULT_DATA_VOLUME;
    }

    /**
     * Calculate recommended parallelism based on the assumption that each core processes 1000 records per second
     *
     * @return recommended parallelism
     */
    private int calculateRecommendedParallelism() {

        long dataVolume = retrieveDataVolume();
        int newParallelism = (int) ceil(dataVolume / MAXIMUM_MESSAGE_PER_SECOND_PER_CORE);
        return Math.max(newParallelism, DEFAULT_PARALLELISM);  // make sure parallelism is at least 1
    }

    /**
     * Get recommended parallelism
     *
     * @return recommended parallelism
     */
    public int getRecommendedParallelism() {
        return calculateRecommendedParallelism();
    }
}
