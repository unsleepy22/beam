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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.core.metrics.MetricsContainerData;
import org.apache.beam.runners.core.metrics.MetricsContainerDataMap;
import org.apache.beam.sdk.metrics.MetricData;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;

/**
 * Spark metrics container map.
 */
public class SparkMetricsContainerStepMap {
    private final ConcurrentHashMap<String, SparkMetricsContainerImpl> metricsContainerStepMap;

    private static SparkMetricsContainerStepMap instance;

    private SparkMetricsContainerStepMap() {
        this.metricsContainerStepMap = new ConcurrentHashMap<>();
    }

    public synchronized static SparkMetricsContainerStepMap getInstance() {
        if (instance == null) {
            instance = new SparkMetricsContainerStepMap();
        }
        return instance;
    }

    public MetricsContainer getMetricsContainer(String stepName) {
        if (!metricsContainerStepMap.containsKey(stepName)) {
            metricsContainerStepMap.put(stepName,
                    new SparkMetricsContainerImpl(stepName));
        }
        return metricsContainerStepMap.get(stepName);
    }

    public void updateMetrics() {
        MetricsContainerDataMap metricsContainerDataMap = new MetricsContainerDataMap();

        for (Map.Entry<String, SparkMetricsContainerImpl> entry : metricsContainerStepMap.entrySet()) {
            String stepName = entry.getKey();
            Map<MetricName, MetricData> updates = entry.getValue().getUpdates();
            metricsContainerDataMap.getContainer(stepName).update(
                    new MetricsContainerData(stepName, updates));
        }

        MetricsAccumulator.getInstance().add(metricsContainerDataMap);
    }
}
