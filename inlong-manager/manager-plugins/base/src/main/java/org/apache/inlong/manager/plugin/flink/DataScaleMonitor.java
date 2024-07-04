package org.apache.inlong.manager.plugin.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.apache.inlong.manager.plugin.flink.FlinkService;
import static java.lang.Math.ceil;

@Slf4j
@Service
public class DataScaleMonitor {

    @Autowired
    private InLongAuditClient inLongAuditClient;

    @Scheduled(fixedRate = 3600000)
    private void scheduleParallelismAdjustmentTask() {

        try {
            long dataVolume = inLongAuditClient.queryDataVolumeLastHour();
            long PARALLELISM_ADJUST_THRESHOLD = 1500;
            if (dataVolume > PARALLELISM_ADJUST_THRESHOLD) {
                adjustParallelism(dataVolume);
            }
        } catch (Exception e) {
            // log error
        }
    }


    private void adjustParallelism(long dataVolume, String jobId, ClusterClient<?> cluster) {
        try {
            long dataVolumeLastHour = dataVolume;
            int newParallelism = calculateNewParallelism(dataVolumeLastHour);

            // Fetch current parallelism of the job using JobID
            int currentParallelism = cluster.getJob(new JobID(StringUtils.hexStringToByte(jobId)))
                    .getJobVertex("YourJobVertexName").getParallelism();

            if (Math.abs(newParallelism - currentParallelism) > 1) {
                updateJobParallelism(newParallelism, jobId, cluster);
            }

        } catch (Exception e) {
            log.error("Failed to adjust parallelism for job " + jobId + " due to an exception.", e);
        }
    }


    private int calculateNewParallelism(double dataVolumeLastHour) {

        // Calculate the new parallelism based on the data volume
        int newParallelism = (int) ceil(dataVolumeLastHour / 1000);
        if (newParallelism < 1) {
            newParallelism = 1;
        }
        return newParallelism;

    }


    private int getJobCurrentParallelism() throws JobExecutionException {

        // Fetch current parallelism of the job using JobID
        return cluster.getJob(jobId).getJobVertex("YourJobVertexName").getParallelism();

    }


    private void updateJobParallelism(int newParallelism) throws Exception {

        //TODO still need to implement this method
        JobVertex vertex = cluster.getJob(jobId).getJobGraph().getVerticesSortedTopologicallyFromSources().get(0);
        vertex.setParallelism(newParallelism);


    }


}
