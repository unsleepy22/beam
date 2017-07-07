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
 * snapshot of a meter.
 */
public class MeterData implements MetricData<MeterData> {
    private static final long serialVersionUID = 1L;

    private double m1;
    private double m5;
    private double m15;
    private double mean;

    public double getM1() {
        return m1;
    }

    public MeterData setM1(double m1) {
        this.m1 = m1;
        return this;
    }

    public double getM5() {
        return m5;
    }

    public MeterData setM5(double m5) {
        this.m5 = m5;
        return this;
    }

    public double getM15() {
        return m15;
    }

    public MeterData setM15(double m15) {
        this.m15 = m15;
        return this;
    }

    public double getMean() {
        return mean;
    }

    public MeterData setMean(double mean) {
        this.mean = mean;
        return this;
    }

    @Override
    public void merge(MeterData other) {
        this.m1 = m1 + other.m1;
        this.m5 = m5 + other.m5;
        this.m15 = m15 + other.m15;
        this.mean = mean + other.mean;
    }
}
