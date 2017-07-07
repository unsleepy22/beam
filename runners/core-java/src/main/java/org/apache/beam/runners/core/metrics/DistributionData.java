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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.metrics.DistributionResult;

/**
 * Data describing the the distribution. This should retain enough detail that it can be combined
 * with other {@link DistributionData}.
 *
 * <p>This is kept distinct from {@link DistributionResult} since this may be extended to include
 * data necessary to approximate quantiles, etc. while {@link DistributionResult} would just include
 * the approximate value of those quantiles.
 */
@AutoValue
public abstract class DistributionData implements Serializable {

  public abstract double p999();
  public abstract double p99();
  public abstract double p95();
  public abstract double p75();
  public abstract double min();
  public abstract double max();
  public abstract double mean();
  public abstract double stddev();

  public static final DistributionData EMPTY = create(0,0,0,0,0,0,0,0);

  public static DistributionData create(double p999, double p99, double p95, double p75,
                                            double min, double max, double mean, double stddev) {
    return new AutoValue_DistributionData(p999, p99, p95, p75, min, max, mean, stddev);
  }

  public static DistributionData singleton(long value) {
    return create(value, value, value, value, value, value, value, 0);
  }

  public DistributionData combine(DistributionData value) {
    return create(
            (p999() + value.p999()) / 2,
            (p99() + value.p99()) / 2,
            (p95() + value.p95()) / 2,
            (p75() + value.p75()) / 2,
            Math.min(value.min(), min()),
            Math.max(value.max(), max()),
            (mean() + value.mean()) / 2,
            0);
  }

  public DistributionResult extractResult() {
    return DistributionResult.create(p999(), p99(), p95(), p75(), min(), max(), mean(), stddev());
  }
}
