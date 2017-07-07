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

package org.apache.beam.sdk.metrics;

/**
 * snapshot of a histogram.
 */
public class HistogramData implements MetricData<HistogramData> {
    private static final long serialVersionUID = 1L;

    private double p999;
    private double p99;
    private double p95;
    private double p75;
    private double mean;
    private double min;
    private double max;
    private double stdDev;
    private long[] values;

    public double getP999() {
        return p999;
    }

    public HistogramData setP999(double p999) {
        this.p999 = p999;
        return this;
    }

    public double getP99() {
        return p99;
    }

    public HistogramData setP99(double p99) {
        this.p99 = p99;
        return this;
    }

    public double getP95() {
        return p95;
    }

    public HistogramData setP95(double p95) {
        this.p95 = p95;
        return this;
    }

    public double getP75() {
        return p75;
    }

    public HistogramData setP75(double p75) {
        this.p75 = p75;
        return this;
    }

    public double getMean() {
        return mean;
    }

    public HistogramData setMean(double mean) {
        this.mean = mean;
        return this;
    }

    public double getMin() {
        return min;
    }

    public HistogramData setMin(double min) {
        this.min = min;
        return this;
    }

    public double getMax() {
        return max;
    }

    public HistogramData setMax(double max) {
        this.max = max;
        return this;
    }

    public double getStdDev() {
        return stdDev;
    }

    public HistogramData setStdDev(double stdDev) {
        this.stdDev = stdDev;
        return this;
    }

    public long[] getValues() {
        return values;
    }

    public HistogramData setValues(long[] values) {
        this.values = values;
        return this;
    }

    @Override
    public void merge(HistogramData other) {
        this.min = Math.min(min, other.min);
        this.max = Math.max(max, other.max);
    }
}
