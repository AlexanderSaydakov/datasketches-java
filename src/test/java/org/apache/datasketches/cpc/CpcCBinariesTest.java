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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;

import java.nio.file.Path;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;

import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * Checks sketch images obtained from C++.
 * @author Lee Rhodes
 */
public class CpcCBinariesTest {
  static PrintStream ps = System.out;
  static final String LS = System.getProperty("line.separator");

  @Test(groups = {CHECK_CPP_FILES})
  public void checkEmptyBin() {
    final Path path = cppPath.resolve("cpc-empty.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory wmem = mh.get();
      println(PreambleUtil.toString(wmem, true));
      final CpcSketch sk = CpcSketch.heapify(wmem);
      assertEquals(sk.getFlavor(), Flavor.EMPTY);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkSparseBin() {
    final Path path = cppPath.resolve("cpc-sparse.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      println("CPP GENERATED SKETCH FROM BINARY FILE LgK=11, U0 to U99");
      println("PreambleUtil.toString(mem, true)" + LS);
      println(PreambleUtil.toString(mem, true));

      println(LS + LS + "################");
      println("CpcSketch sk = CpcSketch.heapify(mem);");
      println("sk.toString(true)" + LS);
      final CpcSketch sk = CpcSketch.heapify(mem);
      println(sk.toString(true));
      assertEquals(sk.getFlavor(), Flavor.SPARSE);
      final double est1 = sk.getEstimate();
      assertEquals(est1, 100, 100 * .02);
      for (int i = 0; i < 100; i++) { sk.update(i); }
      final double est2 = sk.getEstimate();
      assertEquals(est2, est1, 0); //assert no change
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkHybridBin() {
    final Path path = cppPath.resolve("cpc-hybrid.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      println("CPP GENERATED SKETCH FROM BINARY FILE LgK=11, U0 to U199");
      println("PreambleUtil.toString(mem, true)" + LS);
      println(PreambleUtil.toString(mem, true));

      println(LS + LS + "################");
      println("CpcSketch sk = CpcSketch.heapify(mem);");
      println("sk.toString(true)" + LS);
      final CpcSketch sk = CpcSketch.heapify(mem);
      println(sk.toString(true));
      assertEquals(sk.getFlavor(), Flavor.HYBRID);
      final double est1 = sk.getEstimate();
      assertEquals(est1, 200, 200 * .02);
      for (long i = 0; i < 200; i++) { sk.update(i); }
      final double est2 = sk.getEstimate();
      assertEquals(est2, est1, 0); //assert no change
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkPinnedBin() {
    final Path path = cppPath.resolve("cpc-pinned.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      println("CPP GENERATED SKETCH FROM BINARY FILE LgK=11, U0 to U1999");
      println("PreambleUtil.toString(mem, true)" + LS);
      println(PreambleUtil.toString(mem, true));

      println(LS + LS + "################");
      println("CpcSketch sk = CpcSketch.heapify(mem);");
      println("sk.toString(true)" + LS);
      final CpcSketch sk = CpcSketch.heapify(mem);
      println(sk.toString(true));
      assertEquals(sk.getFlavor(), Flavor.PINNED);
      final double est1 = sk.getEstimate();
      assertEquals(est1, 2000, 2000 * .02);
      for (long i = 0; i < 2000; i++) { sk.update(i); }
      final double est2 = sk.getEstimate();
      assertEquals(est2, est1, 0); //assert no change
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkSlidingBin() {
    final Path path = cppPath.resolve("cpc-sliding.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      println("CPP GENERATED SKETCH FROM BINARY FILE LgK=11, U0 to U19999");
      println("PreambleUtil.toString(mem, true)" + LS);
      println(PreambleUtil.toString(mem, true));

      println(LS + LS + "################");
      println("CpcSketch sk = CpcSketch.heapify(mem);");
      println("sk.toString(true)" + LS);
      final CpcSketch sk = CpcSketch.heapify(mem);
      println(sk.toString(true));
      assertEquals(sk.getFlavor(), Flavor.SLIDING);
      final double est1 = sk.getEstimate();
      assertEquals(est1, 20000, 20000 * .02);
      for (long i = 0; i < 20000; i++) { sk.update(i); }
      final double est2 = sk.getEstimate();
      assertEquals(est2, est1, 0);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  //Image checks

  @Test(groups = {CHECK_CPP_FILES})
  public void checkEmptyImages() {
    final Path path = cppPath.resolve("cpc-empty.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      final int cap = (int) mem.getCapacity();
      final byte[] memByteArr = new byte[cap];
      mem.getByteArray(0, memByteArr, 0, cap);

      final CpcSketch sk = new CpcSketch(11);
      final byte[] mem2ByteArr = sk.toByteArray();
      final Memory mem2 = Memory.wrap(mem2ByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(memByteArr, mem2ByteArr);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkSparseImages() {
    final Path path = cppPath.resolve("cpc-sparse.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      final int cap = (int) mem.getCapacity();
      final byte[] memByteArr = new byte[cap];
      mem.getByteArray(0, memByteArr, 0, cap);

      final CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < 100; i++) { sk.update(i); }
      final byte[] mem2ByteArr = sk.toByteArray();
      final Memory mem2 = Memory.wrap(mem2ByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(memByteArr, mem2ByteArr);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkHybridImages() {
    final Path path = cppPath.resolve("cpc-hybrid.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      final int cap = (int) mem.getCapacity();
      final byte[] memByteArr = new byte[cap];
      mem.getByteArray(0, memByteArr, 0, cap);

      final CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < 200; i++) { sk.update(i); }
      final byte[] mem2ByteArr = sk.toByteArray();
      final Memory mem2 = Memory.wrap(mem2ByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(memByteArr, mem2ByteArr);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkPinnedImages() {
    final Path path = cppPath.resolve("cpc-pinned.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      final int cap = (int) mem.getCapacity();
      final byte[] cppMemByteArr = new byte[cap];
      mem.getByteArray(0, cppMemByteArr, 0, cap);

      final CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < 2000; i++) { sk.update(i); }
      final byte[] javaMemByteArr = sk.toByteArray();
      final Memory mem2 = Memory.wrap(javaMemByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(cppMemByteArr, javaMemByteArr);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkSlidingImages() {
    final Path path = cppPath.resolve("cpc-sliding.sk");
    try (MapHandle mh = Memory.map(path.toFile())) {
      final Memory mem = mh.get();
      final int cap = (int) mem.getCapacity();
      final byte[] memByteArr = new byte[cap];
      mem.getByteArray(0, memByteArr, 0, cap);

      final CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < 20000; i++) { sk.update(i); }
      final byte[] mem2ByteArr = sk.toByteArray();
      final Memory mem2 = Memory.wrap(mem2ByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(memByteArr, mem2ByteArr);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test //Internal consistency check
  public void genSparseSketch() {
    final CpcSketch sk = new CpcSketch(11);
    for (int i = 0; i < 100; i++) { sk.update(i); }
    println("JAVA GENERATED SKETCH LgK=11, U0 to U99");
    println("sketch.toString(true);" + LS);
    println(sk.toString(true));

    println(LS + LS + "################");
    final byte[] byteArray = sk.toByteArray();
    println("sketch.toByteArray();");
    println("PreambleUtil.toString(byteArray, true);" + LS);
    println(PreambleUtil.toString(byteArray, true));

    println(LS + LS + "################");
    println("CpcSketch sk2 = CpcSketch.heapify(byteArray);");
    println("sk2.toString(true);" + LS);
    final CpcSketch sk2 = CpcSketch.heapify(byteArray);
    println(sk2.toString(true));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 100, 200, 2000, 20_000};
    final Flavor[] flavorArr = {Flavor.EMPTY, Flavor.SPARSE, Flavor.HYBRID, Flavor.PINNED, Flavor.SLIDING};
    int flavorIdx = 0;
    for (int n: nArr) {
      final CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < n; i++) sk.update(i);
      assertEquals(sk.getFlavor(), flavorArr[flavorIdx++]);
      Files.newOutputStream(javaPath.resolve("cpc_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param format the string to print
   * @param args the arguments
   */
  static void printf(final String format, final Object... args) {
    //ps.printf(format, args);
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //ps.println(s); //disable here
  }

}
