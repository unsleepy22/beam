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

package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.Meter;
import org.apache.beam.sdk.metrics.MeterData;
import org.apache.beam.sdk.metrics.MetricName;

/**
 * Tracks the current value (and delta) for a Counter metric for a specific context and bundle.
 *
 * <p>This class generally shouldn't be used directly. The only exception is within a runner where
 * a counter is being reported for a specific step (rather than the counter in the current context).
 * In that case retrieving the underlying cell and reporting directly to it avoids a step of
 * indirection.
 */
@Experimental(Kind.METRICS)
public class MeterCell implements Meter, MetricCell<MeterData> {

  private final DirtyState dirty = new DirtyState();
  private final com.codahale.metrics.Meter value = new com.codahale.metrics.Meter();
  private final MetricName name;

  /**
   * Package-visibility because all {@link MeterCell CounterCells} should be created by
   * {@link MetricsContainerImpl#getCounter(MetricName)}.
   */
  MeterCell(MetricName name) {
    this.name = name;
  }

  /**
   * Increment the counter by the given amount.
   * @param n value to increment by. Can be negative to decrement.
   */
  @Override
  public void mark(long n) {
    value.mark(n);
    dirty.afterModification();
  }

  public void mark() {
    mark(1);
  }

  @Override
  public DirtyState getDirty() {
    return dirty;
  }

  @Override
  public MeterData getCumulative() {
      MeterData meterData = new MeterData();
      meterData.setM1(value.getOneMinuteRate());
      meterData.setM5(value.getFiveMinuteRate());
      meterData.setM15(value.getFifteenMinuteRate());
      meterData.setMean(value.getMeanRate());

      return meterData;
  }

  @Override
  public MetricName getName() {
    return name;
  }

  void update(MeterData data) {
    // todo: how to update a meter?
    dirty.afterModification();
  }
}
