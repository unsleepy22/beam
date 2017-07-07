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

import org.apache.beam.runners.core.metrics.MetricsContainerDataMap;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * Accumulator of {@link MetricsContainerStepMap}.
 */
public class MetricsAccumulator implements SimpleAccumulator<MetricsContainerDataMap> {
  private static final long serialVersionUID = 1L;

  private MetricsContainerDataMap metricsContainerDataMap = new MetricsContainerDataMap();

  @Override
  public void add(MetricsContainerDataMap value) {
    metricsContainerDataMap.updateAll(value);
  }

  @Override
  public MetricsContainerDataMap getLocalValue() {
    return metricsContainerDataMap;
  }

  @Override
  public void resetLocal() {
    this.metricsContainerDataMap = new MetricsContainerDataMap();
  }

  @Override
  public void merge(Accumulator<MetricsContainerDataMap, MetricsContainerDataMap> other) {
    this.add(other.getLocalValue());
  }

  @Override
  public Accumulator<MetricsContainerDataMap, MetricsContainerDataMap> clone() {
    try {
      super.clone();
    } catch (CloneNotSupportedException ignored) {
    }
    MetricsAccumulator metricsAccumulator = new MetricsAccumulator();
    metricsAccumulator.getLocalValue().updateAll(this.getLocalValue());
    return metricsAccumulator;
  }
}
