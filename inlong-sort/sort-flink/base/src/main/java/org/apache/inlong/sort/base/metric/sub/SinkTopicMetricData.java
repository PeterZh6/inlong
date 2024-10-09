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

package org.apache.inlong.sort.base.metric.sub;

import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.SinkExactlyMetric;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.base.Constants.DELIMITER;
import static org.apache.inlong.sort.base.Constants.DIRTY_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.DIRTY_RECORDS_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;

public class SinkTopicMetricData extends SinkExactlyMetric implements SinkSubMetricData {

    public static final Logger LOGGER = LoggerFactory.getLogger(SinkTopicMetricData.class);

    /**
     * The sink metric data map
     */
    private final Map<String, SinkExactlyMetric> topicSinkMetricMap = Maps.newHashMap();

    public SinkTopicMetricData(MetricOption option, MetricGroup metricGroup) {
        super(option, metricGroup);
    }

    /**
     * register sub sink metrics group from metric state
     *
     * @param metricState MetricState
     */
    public void registerSubMetricsGroup(MetricState metricState) {
        if (metricState == null) {
            return;
        }

        // register sub sink metric data
        if (metricState.getSubMetricStateMap() == null) {
            return;
        }
        Map<String, MetricState> subMetricStateMap = metricState.getSubMetricStateMap();
        for (Entry<String, MetricState> subMetricStateEntry : subMetricStateMap.entrySet()) {
            String topic = subMetricStateEntry.getKey();
            final MetricState subMetricState = subMetricStateEntry.getValue();
            SinkExactlyMetric subSinkMetricData = buildSinkExactlyMetric(topic, subMetricState, this);
            topicSinkMetricMap.put(topic, subSinkMetricData);
        }
        LOGGER.info("register topicMetricsGroup from metricState,topic level metric map size:{}",
                topicSinkMetricMap.size());
    }

    public void sendOutMetrics(String topic, long rowCount, long rowSize) {
        if (StringUtils.isBlank(topic)) {
            invoke(rowCount, rowSize, System.currentTimeMillis());
            return;
        }
        SinkExactlyMetric sinkExactlyMetric = getSinkExactlyMetric(topic);

        this.invoke(rowCount, rowSize, System.currentTimeMillis());
        sinkExactlyMetric.invoke(rowCount, rowSize, System.currentTimeMillis());
    }

    public void sendDirtyMetrics(String topic, long rowCount, long rowSize) {
        if (StringUtils.isBlank(topic)) {
            invokeDirty(rowCount, rowSize);
            return;
        }
        SinkExactlyMetric sinkExactlyMetric = getSinkExactlyMetric(topic);

        this.invokeDirty(rowCount, rowSize);
        sinkExactlyMetric.invokeDirty(rowCount, rowSize);
    }

    private SinkExactlyMetric getSinkExactlyMetric(String topic) {
        SinkExactlyMetric sinkExactlyMetric;
        if (topicSinkMetricMap.containsKey(topic)) {
            sinkExactlyMetric = topicSinkMetricMap.get(topic);
        } else {
            sinkExactlyMetric = buildSinkExactlyMetric(topic, null, this);
            topicSinkMetricMap.put(topic, sinkExactlyMetric);
        }
        return sinkExactlyMetric;
    }

    private SinkExactlyMetric buildSinkExactlyMetric(String topic, MetricState metricState,
            SinkExactlyMetric sinkExactlyMetric) {
        Map<String, String> labels = sinkExactlyMetric.getLabels();
        String metricGroupLabels = labels.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(DELIMITER));

        MetricOption metricOption = MetricOption.builder()
                .withInlongLabels(metricGroupLabels + DELIMITER + Constants.TOPIC_NAME + "=" + topic)
                .withInitRecords(metricState != null ? metricState.getMetricValue(NUM_RECORDS_OUT) : 0L)
                .withInitBytes(metricState != null ? metricState.getMetricValue(NUM_BYTES_OUT) : 0L)
                .withInitDirtyRecords(metricState != null ? metricState.getMetricValue(DIRTY_RECORDS_OUT) : 0L)
                .withInitDirtyBytes(metricState != null ? metricState.getMetricValue(DIRTY_BYTES_OUT) : 0L)
                .withRegisterMetric(RegisteredMetric.ALL)
                .build();
        return new SinkExactlyMetric(metricOption, sinkExactlyMetric.getMetricGroup());
    }

    @Override
    public Map<String, SinkExactlyMetric> getSubSinkMetricMap() {
        return this.topicSinkMetricMap;
    }

    @Override
    public String toString() {
        return "SinkTopicMetricData{"
                + super.toString() + ","
                + "subSinkMetricMap=" + topicSinkMetricMap
                + '}';
    }
}
