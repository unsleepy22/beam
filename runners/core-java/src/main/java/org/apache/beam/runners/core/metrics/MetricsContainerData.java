package org.apache.beam.runners.core.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.metrics.MetricData;
import org.apache.beam.sdk.metrics.MetricName;

/**
 * metrics data within a metrics container.
 */
public class MetricsContainerData {

    private final String stepName;
    private ConcurrentHashMap<MetricName, MetricData> metricSnapshots = new ConcurrentHashMap<>();

    public MetricsContainerData(String stepName) {
        this(stepName, new ConcurrentHashMap<MetricName, MetricData>());
    }

    public MetricsContainerData(String stepName, Map<MetricName, MetricData> metricSnapshots) {
        this.stepName = stepName;
        this.metricSnapshots.putAll(metricSnapshots);
    }

    public void update(MetricsContainerData containerData) {
        for (Map.Entry<MetricName, MetricData> snapshot : containerData.metricSnapshots.entrySet()) {
            MetricName metricName = snapshot.getKey();
            MetricData metricData = snapshot.getValue();

            if (metricSnapshots.containsKey(metricName)) {
                metricSnapshots.get(metricName).merge(metricData);
            } else {
                metricSnapshots.put(metricName, metricData);
            }
        }
    }

    public Map<MetricName, MetricData> getMetricSnapshots() {
        return metricSnapshots;
    }
}
