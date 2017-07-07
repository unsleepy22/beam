/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.flink.metrics;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.metrics.DirtyState;
import org.apache.beam.runners.core.metrics.MetricCell;
import org.apache.beam.runners.core.metrics.MetricsMap;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.CounterData;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.GaugeData;
import org.apache.beam.sdk.metrics.HistogramData;
import org.apache.beam.sdk.metrics.Meter;
import org.apache.beam.sdk.metrics.MeterData;
import org.apache.beam.sdk.metrics.MetricData;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.HistogramStatistics;

/**
 * Holds the metrics for a single step and unit-of-commit (bundle).
 *
 * <p>This class is thread-safe. It is intended to be used with 1 (or more) threads are updating
 * metrics and at-most 1 thread is extracting updates by calling {@link #getUpdates} and
 * {@link #commitUpdates}. Outside of this it is still safe. Although races in the update extraction
 * may cause updates that don't actually have any changes, it will never lose an update.
 *
 * <p>For consistency, all threads that update metrics should finish before getting the final
 * cumulative values/updates.
 */
@Experimental(Kind.METRICS)
public class FlinkMetricsContainerImpl implements Serializable, MetricsContainer {

    private final String stepName;
    private final RuntimeContext runtimeContext;

    private final MetricsMap<MetricName, FlinkCounter> counters =
            new MetricsMap<>(new MetricsMap.Factory<MetricName, FlinkCounter>() {
                @Override
                public FlinkCounter createInstance(MetricName name) {
                    FlinkCounter counter = new FlinkCounter(name);
                    runtimeContext.getMetricGroup().counter(getFlinkMetricName(name), counter);
                    return counter;
                }
            });

    private final MetricsMap<MetricName, FlinkMeter> meters =
            new MetricsMap<>(new MetricsMap.Factory<MetricName, FlinkMeter>() {
                @Override
                public FlinkMeter createInstance(MetricName name) {
                    FlinkMeter meter = new FlinkMeter(name);
                    runtimeContext.getMetricGroup().meter(getFlinkMetricName(name), meter);
                    return meter;
                }
            });

    private final MetricsMap<MetricName, FlinkHistogram> distributions =
            new MetricsMap<>(new MetricsMap.Factory<MetricName, FlinkHistogram>() {
                @Override
                public FlinkHistogram createInstance(MetricName name) {
                    FlinkHistogram histogram = new FlinkHistogram(name);
                    runtimeContext.getMetricGroup().histogram(getFlinkMetricName(name), histogram);
                    return histogram;
                }
            });

    private final MetricsMap<MetricName, FlinkGauge> gauges =
            new MetricsMap<>(new MetricsMap.Factory<MetricName, FlinkGauge>() {
                @Override
                public FlinkGauge createInstance(MetricName name) {
                    FlinkGauge gauge = new FlinkGauge(name);
                    runtimeContext.getMetricGroup().gauge(getFlinkMetricName(name), gauge);
                    return gauge;
                }
            });

    /**
     * Create a new {@link FlinkMetricsContainerImpl} associated with the given {@code stepName}.
     */
    public FlinkMetricsContainerImpl(String stepName, RuntimeContext runtimeContext) {
        this.stepName = stepName;
        this.runtimeContext = runtimeContext;
    }

    private static final String DELIM = "/";

    private String getFlinkMetricName(MetricName metricName) {
        // do not include stepName as it sometimes grows too long
        return StringUtils.isBlank(metricName.namespace()) ? metricName.name() :
                metricName.namespace() + DELIM + metricName.name();
    }

    @Override
    public FlinkCounter getCounter(MetricName metricName) {
        return counters.get(metricName);
    }

    @Override
    public FlinkHistogram getDistribution(MetricName metricName) {
        return distributions.get(metricName);
    }

    @Override
    public FlinkGauge getGauge(MetricName metricName) {
        return gauges.get(metricName);
    }

    @Override
    public FlinkMeter getMeter(MetricName metricName) {
        return meters.get(metricName);
    }

    /**
     * Return the cumulative values for any metrics that have changed since the last time updates were
     * committed.
     */
    public Map<MetricName, MetricData> getUpdates() {
        Map<MetricName, MetricData> ret = new HashMap<>();

        for (Map.Entry<MetricName, FlinkCounter> entry : counters.entries()) {
            ret.put(entry.getKey(), entry.getValue().getCumulative());
        }

        for (Map.Entry<MetricName, FlinkGauge> entry : gauges.entries()) {
            ret.put(entry.getKey(), entry.getValue().getCumulative());
        }

        for (Map.Entry<MetricName, FlinkMeter> entry : meters.entries()) {
            // ignore m5, m15 and mean rate because flink takes m1 rate only
            ret.put(entry.getKey(), entry.getValue().getCumulative());
        }

        for (Map.Entry<MetricName, FlinkHistogram> entry : distributions.entries()) {
            ret.put(entry.getKey(), entry.getValue().getCumulative());
        }

        return ret;
    }


    public static class FlinkCounter implements Counter, MetricCell<CounterData>,
            org.apache.flink.metrics.Counter {
        private final com.codahale.metrics.Counter value = new com.codahale.metrics.Counter();
        private final MetricName name;
        private final DirtyState dirty = new DirtyState();

        public FlinkCounter(MetricName name) {
            this.name = name;
        }

        @Override
        public void inc() {
            inc(1);
        }

        @Override
        public void inc(long n) {
            value.inc(n);
            dirty.afterModification();
        }

        @Override
        public void dec() {
            inc(-1);
        }

        @Override
        public void dec(long n) {
            inc(-n);
        }

        @Override
        public long getCount() {
            return value.getCount();
        }

        @Override
        public MetricName getName() {
            return name;
        }

        @Override
        public DirtyState getDirty() {
            return dirty;
        }

        @Override
        public CounterData getCumulative() {
            return new CounterData().setValue(value.getCount());
        }
    }

    public static class FlinkMeter implements Meter, MetricCell<MeterData>,
            org.apache.flink.metrics.Meter {
        private final com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();
        private final MetricName name;
        private final DirtyState dirty = new DirtyState();

        public FlinkMeter(MetricName name) {
            this.name = name;
        }

        @Override
        public void mark() {
            meter.mark();
        }

        @Override
        public void mark(long n) {
            meter.mark(n);
        }

        @Override
        public MetricName getName() {
            return name;
        }

        @Override
        public DirtyState getDirty() {
            return dirty;
        }

        @Override
        public MeterData getCumulative() {
            MeterData meterData = new MeterData();
            meterData.setM1(meter.getOneMinuteRate());
            meterData.setM5(meter.getFiveMinuteRate());
            meterData.setM15(meter.getFifteenMinuteRate());
            meterData.setMean(meter.getMeanRate());

            return meterData;
        }

        @Override
        public void markEvent() {
            mark();
        }

        @Override
        public void markEvent(long l) {
            mark(l);
        }

        @Override
        public double getRate() {
            return meter.getMeanRate();
        }

        @Override
        public long getCount() {
            return meter.getCount();
        }
    }

    public static class FlinkHistogram implements Distribution, MetricCell<HistogramData>,
            org.apache.flink.metrics.Histogram {
        private final com.codahale.metrics.Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir());
        private final MetricName name;
        private final DirtyState dirty = new DirtyState();

        public FlinkHistogram(MetricName name) {
            this.name = name;
        }

        @Override
        public void update(long value) {
            histogram.update(value);
        }

        @Override
        public long getCount() {
            return histogram.getCount();
        }

        @Override
        public HistogramStatistics getStatistics() {
            return new DropwizardHistogramStatistics(histogram.getSnapshot());
        }

        @Override
        public MetricName getName() {
            return name;
        }

        @Override
        public DirtyState getDirty() {
            return dirty;
        }

        @Override
        public HistogramData getCumulative() {
            HistogramData histogramData = new HistogramData();
            Snapshot snapshot = histogram.getSnapshot();
            histogramData.setP99(snapshot.get99thPercentile());
            histogramData.setP95(snapshot.get95thPercentile());
            histogramData.setP75(snapshot.get75thPercentile());
            histogramData.setMax(snapshot.getMax());
            histogramData.setMin(snapshot.getMin());
            histogramData.setMean(snapshot.getMean());
            histogramData.setStdDev(snapshot.getStdDev());
            histogramData.setValues(snapshot.getValues());

            return histogramData;
        }
    }

    public static class FlinkGauge implements Gauge, MetricCell<GaugeData>,
            org.apache.flink.metrics.Gauge<Long> {
        private final AtomicLong value = new AtomicLong(0L);
        private final MetricName name;
        private final DirtyState dirty = new DirtyState();

        public FlinkGauge(MetricName name) {
            this.name = name;
        }

        @Override
        public void set(long value) {
            this.value.set(value);
            dirty.afterModification();
        }

        @Override
        public MetricName getName() {
            return name;
        }

        @Override
        public DirtyState getDirty() {
            return dirty;
        }

        @Override
        public GaugeData getCumulative() {
            return new GaugeData().setValue(value.get());
        }

        @Override
        public Long getValue() {
            return value.get();
        }
    }
}
