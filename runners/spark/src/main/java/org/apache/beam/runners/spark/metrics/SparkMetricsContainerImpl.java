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

package org.apache.beam.runners.spark.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.metrics.DirtyState;
import org.apache.beam.runners.core.metrics.MetricCell;
import org.apache.beam.runners.core.metrics.MetricsMap;
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
import org.apache.commons.lang3.StringUtils;

/**
 * Spark metrics container implementation for a specific step.
 */
public class SparkMetricsContainerImpl implements MetricsContainer {
    private static final long serialVersionUID = 1L;

    private static final String DELIM = "/";
    private final String stepName;
    private final MetricRegistry metricRegistry = SparkBeamMetric.getMetricRegistry();

    public SparkMetricsContainerImpl(String stepName) {
        this.stepName = stepName;
    }

    private String getMetricName(MetricName name) {
        return stepName + DELIM +
                (StringUtils.isBlank(name.namespace()) ? "-" : name.namespace()) +
                DELIM + name.name();
    }

    private MetricsMap<MetricName, SparkCounter> counters =
            new MetricsMap<>(new MetricsMap.Factory<MetricName, SparkCounter>() {
                @Override
                public SparkCounter createInstance(MetricName name) {
                    com.codahale.metrics.Counter counter = metricRegistry.counter(getMetricName(name));
                    SparkCounter sparkCounter = new SparkCounter(name, counter);
                    return sparkCounter;
                }
            });

    private MetricsMap<MetricName, SparkMeter> meters =
            new MetricsMap<>(new MetricsMap.Factory<MetricName, SparkMeter>() {
                @Override
                public SparkMeter createInstance(MetricName name) {
                    com.codahale.metrics.Meter meter = metricRegistry.meter(getMetricName(name));
                    return new SparkMeter(name, meter);
                }
            });

    private MetricsMap<MetricName, SparkHistogram> distributions =
            new MetricsMap<>(new MetricsMap.Factory<MetricName, SparkHistogram>() {
                @Override
                public SparkHistogram createInstance(MetricName name) {
                    Histogram histogram = metricRegistry.histogram(getMetricName(name));
                    return new SparkHistogram(name, histogram);
                }
            });

    private MetricsMap<MetricName, SparkGauge> gauges =
            new MetricsMap<>(new MetricsMap.Factory<MetricName, SparkGauge>() {
                @Override
                public SparkGauge createInstance(MetricName name) {
                    final SparkGauge gauge = new SparkGauge(name);
                    metricRegistry.register(getMetricName(name), new com.codahale.metrics.Gauge<Long>() {
                        @Override
                        public Long getValue() {
                            return gauge.getValue();
                        }
                    });
                    return gauge;
                }
            });


    @Override
    public Counter getCounter(MetricName metricName) {
        return counters.get(metricName);
    }

    @Override
    public Distribution getDistribution(MetricName metricName) {
        return distributions.get(metricName);
    }

    @Override
    public Gauge getGauge(MetricName metricName) {
        return gauges.get(metricName);
    }

    @Override
    public Meter getMeter(MetricName metricName) {
        return meters.get(metricName);
    }

    public Map<MetricName, MetricData> getUpdates() {
        Map<MetricName, MetricData> ret = new HashMap<>();

        for (Map.Entry<MetricName, SparkCounter> entry : counters.entries()) {
            ret.put(entry.getKey(), entry.getValue().getCumulative());
        }

        for (Map.Entry<MetricName, SparkGauge> entry : gauges.entries()) {
            ret.put(entry.getKey(), entry.getValue().getCumulative());
        }

        for (Map.Entry<MetricName, SparkMeter> entry : meters.entries()) {
            // ignore m5, m15 and mean rate
            ret.put(entry.getKey(), entry.getValue().getCumulative());
        }

        for (Map.Entry<MetricName, SparkHistogram> entry : distributions.entries()) {
            ret.put(entry.getKey(), entry.getValue().getCumulative());
        }

        return ret;
    }


    public static class SparkCounter implements Counter, MetricCell<CounterData> {
        private static final long serialVersionUID = 1L;

        private final com.codahale.metrics.Counter value;
        private final MetricName name;
        private final DirtyState dirty = new DirtyState();

        public SparkCounter(MetricName name, com.codahale.metrics.Counter counter) {
            this.name = name;
            this.value = counter;
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

    public static class SparkMeter implements Meter, MetricCell<MeterData> {
        private static final long serialVersionUID = 1L;

        private final com.codahale.metrics.Meter meter;
        private final MetricName name;
        private final DirtyState dirty = new DirtyState();

        public SparkMeter(MetricName name, com.codahale.metrics.Meter meter) {
            this.name = name;
            this.meter = meter;
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
    }

    public static class SparkHistogram implements Distribution, MetricCell<HistogramData> {
        private static final long serialVersionUID = 1L;

        private final com.codahale.metrics.Histogram histogram;
        private final MetricName name;
        private final DirtyState dirty = new DirtyState();

        public SparkHistogram(MetricName name, Histogram histogram) {
            this.name = name;
            this.histogram = histogram;
        }

        @Override
        public void update(long value) {
            histogram.update(value);
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

            return histogramData;
        }
    }

    public static class SparkGauge implements Gauge, MetricCell<GaugeData> {
        private static final long serialVersionUID = 1L;

        private final AtomicLong value;
        private final MetricName name;
        private final DirtyState dirty = new DirtyState();

        public SparkGauge(MetricName name) {
            this.name = name;
            this.value = new AtomicLong(0L);
        }

        @Override
        public void set(long value) {
            dirty.afterModification();
            this.value.set(value);
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

        public long getValue() {
            return this.value.get();
        }
    }
}
