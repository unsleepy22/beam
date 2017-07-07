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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The result of a {@link Distribution} metric.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class DistributionResult {

  public abstract double p999();
  public abstract double p99();
  public abstract double p95();
  public abstract double p75();
  public abstract double min();
  public abstract double max();
  public abstract double mean();
  public abstract double stddev();

  public static final DistributionResult ZERO = create(0, 0, 0, 0, 0, 0, 0, 0);

  public static DistributionResult create(double p999, double p99, double p95, double p75,
                                          double min, double max, double mean, double stddev) {
    return new AutoValue_DistributionResult(p999, p99, p95, p75, min, max, mean, stddev);
  }
}
