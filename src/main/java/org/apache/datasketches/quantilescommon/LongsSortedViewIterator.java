/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.quantilescommon;

/**
 * Iterator over quantile sketches of primitive type <i>long</i>.
 */
public final class LongsSortedViewIterator extends SortedViewIterator {
  private final long[] quantiles;

  /**
   * Constructor.
   * @param quantiles the given array of quantiles, which must be ordered.
   * @param cumWeights the given array of cumulative weights, which must be ordered, start with the value one, and
   * the last value must be equal to N, the total number of items updated to the sketch.
   */
  public LongsSortedViewIterator(final long[] quantiles, final long[] cumWeights) {
    super(cumWeights);
    this.quantiles = quantiles; //SpotBugs EI_EXPOSE_REP2 suppressed by FindBugsExcludeFilter
  }

  /**
   * Gets the quantile at the current index.
   *
   * <p>Don't call this before calling next() for the first time
   * or after getting false from next().</p>
   *
   * @return the quantile at the current index.
   */
  public long getQuantile() {
    return quantiles[index];
  }

}
