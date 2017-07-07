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

import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.core.metrics.MetricsContainerDataMap;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for holding a {@link MetricsContainerImpl} and forwarding Beam metrics to
 * Flink accumulators and metrics.
 */
public class FlinkMetricContainer {

    public static final String ACCUMULATOR_NAME = "__metricscontainers";

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMetricContainer.class);

    private final RuntimeContext runtimeContext;
    private final MetricsAccumulator metricsAccumulator;
    private final ConcurrentHashMap<String, FlinkMetricsContainerImpl> metricsContainerStepMap;

    public FlinkMetricContainer(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;

        Accumulator<MetricsContainerDataMap, MetricsContainerDataMap> metricsAccumulator =
                runtimeContext.getAccumulator(ACCUMULATOR_NAME);
        if (metricsAccumulator == null) {
            metricsAccumulator = new MetricsAccumulator();
            try {
                runtimeContext.addAccumulator(ACCUMULATOR_NAME, metricsAccumulator);
            } catch (Exception e) {
                LOG.error("Failed to create metrics accumulator.", e);
            }
        }
        this.metricsAccumulator = (MetricsAccumulator) metricsAccumulator;
        this.metricsContainerStepMap = new ConcurrentHashMap<>();
    }

    public MetricsContainer getMetricsContainer(String stepName) {
        if (!metricsContainerStepMap.containsKey(stepName)) {
            metricsContainerStepMap.put(stepName,
                    new FlinkMetricsContainerImpl(stepName, runtimeContext));
        }
        return metricsContainerStepMap.get(stepName);
    }

    void updateMetrics() {
//    MetricResults metricResults =
//        asAttemptedOnlyMetricResults(metricsAccumulator.getLocalValue());
//    MetricQueryResults metricQueryResults =
//        metricResults.queryMetrics(MetricsFilter.builder().build());
    }
}
