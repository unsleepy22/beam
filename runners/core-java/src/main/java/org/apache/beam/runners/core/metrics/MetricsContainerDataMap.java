package org.apache.beam.runners.core.metrics;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.metrics.CounterData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.HistogramData;
import org.apache.beam.sdk.metrics.MeterData;
import org.apache.beam.sdk.metrics.MeterResult;
import org.apache.beam.sdk.metrics.MetricData;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.joda.time.Instant;

/**
 * metrics containers for steps.
 */
public class MetricsContainerDataMap implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, MetricsContainerData> containers;

    public MetricsContainerDataMap() {
        this.containers = new ConcurrentHashMap<>();
    }

    public MetricsContainerData getContainer(String stepName) {
        if (!containers.containsKey(stepName)) {
            containers.put(stepName, new MetricsContainerData(stepName));
        }
        return containers.get(stepName);
    }

    /**
     * Update this {@link MetricsContainerStepMap} with all values from given
     * {@link MetricsContainerStepMap}.
     */
    public void updateAll(MetricsContainerDataMap other) {
        for (Map.Entry<String, MetricsContainerData> container : other.containers.entrySet()) {
            getContainer(container.getKey()).update(container.getValue());
        }
    }

    /**
     * Update {@link MetricsContainerImpl} for given step in this map with all values from given
     * {@link MetricsContainerImpl}.
     */
    public void update(String step, MetricsContainerData containerData) {
        getContainer(step).update(containerData);
    }

    public MetricQueryResults queryMetrics(MetricsFilter filter) {
        return new MetricsContainerResults().queryMetrics(filter);
    }

    class MetricsContainerResults extends MetricResults {

        @Override
        public MetricQueryResults queryMetrics(MetricsFilter filter) {
            return new QueryResults(filter);
        }
    }

    class QueryResults implements MetricQueryResults {
        private final MetricsFilter filter;

        private QueryResults(MetricsFilter filter) {
            this.filter = filter;
        }

        @Override
        public Iterable<MetricResult<Long>> counters() {
            List<MetricResult<Long>> ret = Lists.newArrayList();
            for (Map.Entry<String, MetricsContainerData> entry : MetricsContainerDataMap.this.containers.entrySet()) {
                String step = entry.getKey();
                for (Map.Entry<MetricName, MetricData> subEntry : entry.getValue().getMetricSnapshots().entrySet()) {
                    if (matchesFilter(filter, step, subEntry.getKey())) {
                        ret.add((MetricResult<Long>)
                                metricDataToMetricResult(subEntry.getKey(), step, subEntry.getValue()));
                    }
                }
            }
            return ret;
        }

        @Override
        public Iterable<MetricResult<DistributionResult>> distributions() {
            List<MetricResult<DistributionResult>> ret = Lists.newArrayList();
            for (Map.Entry<String, MetricsContainerData> entry : MetricsContainerDataMap.this.containers.entrySet()) {
                String step = entry.getKey();
                for (Map.Entry<MetricName, MetricData> subEntry : entry.getValue().getMetricSnapshots().entrySet()) {
                    if (matchesFilter(filter, step, subEntry.getKey())) {
                        ret.add((MetricResult<DistributionResult>)
                                metricDataToMetricResult(subEntry.getKey(), step, subEntry.getValue()));
                    }
                }
            }
            return ret;
        }

        @Override
        public Iterable<MetricResult<GaugeResult>> gauges() {
            List<MetricResult<GaugeResult>> ret = Lists.newArrayList();
            for (Map.Entry<String, MetricsContainerData> entry : MetricsContainerDataMap.this.containers.entrySet()) {
                String step = entry.getKey();
                for (Map.Entry<MetricName, MetricData> subEntry : entry.getValue().getMetricSnapshots().entrySet()) {
                    if (matchesFilter(filter, step, subEntry.getKey())) {
                        ret.add((MetricResult<GaugeResult>)
                                metricDataToMetricResult(subEntry.getKey(), step, subEntry.getValue()));
                    }
                }
            }
            return ret;
        }

        @Override
        public Iterable<MetricResult<MeterResult>> meters() {
            List<MetricResult<MeterResult>> ret = Lists.newArrayList();
            for (Map.Entry<String, MetricsContainerData> entry : MetricsContainerDataMap.this.containers.entrySet()) {
                String step = entry.getKey();
                for (Map.Entry<MetricName, MetricData> subEntry : entry.getValue().getMetricSnapshots().entrySet()) {
                    if (matchesFilter(filter, step, subEntry.getKey())) {
                        ret.add((MetricResult<MeterResult>)
                                metricDataToMetricResult(subEntry.getKey(), step, subEntry.getValue()));
                    }
                }
            }
            return ret;
        }

        private boolean matchesFilter(final MetricsFilter filter, String stepName, MetricName metricName) {
            if (filter == null) {
                return true;
            }
            return MetricFiltering.matches(filter, MetricKey.create(stepName, metricName));
        }

        private MetricResult<?> metricDataToMetricResult(MetricName name, String step, MetricData data) {
            if (data instanceof CounterData) {
                return new CounterMetricResult(name, step, (CounterData) data);
            } else if (data instanceof MeterData) {
                return new MeterMetricResult(name, step, (MeterData) data);
            } else if (data instanceof HistogramData) {
                return new DistributionMetricResult(name, step, (HistogramData) data);
            } else if (data instanceof GaugeData) {
                return new GaugeMetricResult(name, step, (GaugeData) data);
            }
            throw new IllegalArgumentException("bad metric data type!");
        }

    }

    abstract class MetricResultBase<T> implements MetricResult<T> {
        private MetricName name;
        private String step;

        MetricResultBase(MetricName name, String step) {
            this.name = name;
            this.step = step;
        }

        @Override
        public MetricName name() {
            return this.name;
        }

        @Override
        public String step() {
            return this.step;
        }

    }

    class CounterMetricResult extends MetricResultBase<Long> {
        private long value;

        CounterMetricResult(MetricName name, String step, CounterData data) {
            super(name, step);
            this.value = data.getValue();
        }


        @Override
        public Long committed() {
            return 0L;
        }

        @Override
        public Long attempted() {
            return value;
        }
    }

    class MeterMetricResult extends MetricResultBase<MeterResult> {
        private MeterResult value;

        MeterMetricResult(MetricName name, String step, MeterData data) {
            super(name, step);
            this.value = MeterResult.create(data.getM1(), data.getM5(), data.getM15(), data.getMean());
        }

        @Override
        public MeterResult committed() {
            return null;
        }

        @Override
        public MeterResult attempted() {
            return this.value;
        }
    }

    class DistributionMetricResult extends MetricResultBase<DistributionResult> {
        private DistributionResult value;

        DistributionMetricResult(MetricName name, String step, HistogramData data) {
            super(name, step);
            this.value = DistributionResult.create(data.getP999(), data.getP99(), data.getP95(), data.getP75(),
                    data.getMin(), data.getMax(), data.getMean(), data.getStdDev());
        }

        @Override
        public DistributionResult committed() {
            return null;
        }

        @Override
        public DistributionResult attempted() {
            return this.value;
        }
    }

    class GaugeMetricResult extends MetricResultBase<GaugeResult> {
        private GaugeResult value;

        GaugeMetricResult(MetricName name, String step, GaugeData gaugeData) {
            super(name, step);
            this.value = GaugeResult.create(gaugeData.value(), Instant.now());
        }

        @Override
        public GaugeResult committed() {
            return null;
        }

        @Override
        public GaugeResult attempted() {
            return this.value;
        }
    }
}
