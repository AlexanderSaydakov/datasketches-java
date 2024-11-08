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

package org.apache.datasketches.tuple;

import static org.apache.datasketches.common.TestUtil.*;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.memory.Memory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerialVersion3Test {

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  public void version2Compatibility() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppHistPath.resolve("TupleWithTestIntegerSummary4kTrimmedSerVer2.sk"));
    Sketch<IntegerSummary> sketch1 = Sketches.heapifySketch(Memory.wrap(byteArr), new IntegerSummaryDeserializer());

    // construct the same way
    final int lgK = 12;
    final int K = 1 << lgK;
    final UpdatableSketchBuilder<Integer, IntegerSummary> builder =
            new UpdatableSketchBuilder<>(new IntegerSummaryFactory());
    final UpdatableSketch<Integer, IntegerSummary> updatableSketch = builder.build();
    for (int i = 0; i < 2 * K; i++) {
      updatableSketch.update(i, 1);
    }
    updatableSketch.trim();
    Sketch<IntegerSummary> sketch2 = updatableSketch.compact();

    Assert.assertEquals(sketch1.getRetainedEntries(), sketch2.getRetainedEntries());
    Assert.assertEquals(sketch1.getThetaLong(), sketch2.getThetaLong());
    Assert.assertEquals(sketch1.isEmpty(), sketch2.isEmpty());
    Assert.assertEquals(sketch1.isEstimationMode(), sketch2.isEstimationMode());
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void emptyFromCpp() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppPath.resolve("tuple-int-empty-cpp.sk"));
    Sketch<IntegerSummary> sketch = Sketches.heapifySketch(Memory.wrap(byteArr), new IntegerSummaryDeserializer());
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void singleItemFromCpp() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppPath.resolve("tuple-int-single-cpp.sk"));
    Sketch<IntegerSummary> sketch = Sketches.heapifySketch(Memory.wrap(byteArr), new IntegerSummaryDeserializer());
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void exactModeFromCpp() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppPath.resolve("tuple-int-two-cpp.sk"));
    Sketch<IntegerSummary> sketch = Sketches.heapifySketch(Memory.wrap(byteArr), new IntegerSummaryDeserializer());
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getRetainedEntries(), 2);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void estimationModeFromCpp() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppPath.resolve("tuple-int-est-trim-cpp.sk"));
    Sketch<IntegerSummary> sketch = Sketches.heapifySketch(Memory.wrap(byteArr), new IntegerSummaryDeserializer());
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getRetainedEntries(), 4096);
    Assert.assertTrue(sketch.getThetaLong() < Long.MAX_VALUE);
  }

}
