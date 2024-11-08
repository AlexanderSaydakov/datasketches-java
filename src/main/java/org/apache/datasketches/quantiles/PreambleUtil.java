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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.common.Family.idToFamily;
import static org.apache.datasketches.quantiles.ClassicUtil.LS;
import static org.apache.datasketches.quantiles.ClassicUtil.computeRetainedItems;

import java.nio.ByteOrder;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

//@formatter:off

/**
 * This class defines the serialized data structure and provides access methods for the key fields.
 *
 * <p>The intent of the design of this class was to isolate the detailed knowledge of the bit and
 * byte layout of the serialized form of the sketches derived from the base sketch classes into one place.
 * This allows the possibility of the introduction of different serialization
 * schemes with minimal impact on the rest of the library.</p>
 *
 * <p>
 * LAYOUT: The low significance bytes of this <i>long</i> based data structure are on the right.
 * The multi-byte primitives are stored in native byte order.
 * The single byte fields are treated as unsigned.</p>
 *
 * <p>An empty ItemsSketch, on-heap DoublesSketch or compact off-heap DoublesSketch only require 8
 * bytes. An off-heap UpdateDoublesSketch and all non-empty sketches require at least 16 bytes of
 * preamble.</p>
 *
 * <pre>{@code
 * Long || Start Byte Adr: Common for both DoublesSketch and ItemsSketch
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0          |
 *  0   ||------unused-----|--------K--------|  Flags | FamID  | SerVer | Preamble_Longs |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8          |
 *  1   ||-----------------------------------N_LONG--------------------------------------|
 *
 *  Applies only to DoublesSketch:
 *
 *      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16          |
 *  2   ||---------------------------START OF DATA, MIN_DOUBLE---------------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24          |
 *  3   ||----------------------------------MAX_DOUBLE-----------------------------------|
 *
 *      ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |    32          |
 *  4   ||---------------------------START OF COMBINED BUfFER----------------------------|
 *  }</pre>
 *
 *  @author Lee Rhodes
 */
final class PreambleUtil {

  private PreambleUtil() {}

  // ###### DO NOT MESS WITH THIS FROM HERE ...
  // Preamble byte Addresses
  static final int PREAMBLE_LONGS_BYTE        = 0;
  static final int SER_VER_BYTE               = 1;
  static final int FAMILY_BYTE                = 2;
  static final int FLAGS_BYTE                 = 3;
  static final int K_SHORT                    = 4;  //to 5
  static final int N_LONG                     = 8;  //to 15

  //After Preamble:
  static final int MIN_DOUBLE                 = 16; //to 23 (Only for DoublesSketch)
  static final int MAX_DOUBLE                 = 24; //to 31 (Only for DoublesSketch)
  static final int COMBINED_BUFFER            = 32; //to 39 (Only for DoublesSketch)

  // flag bit masks
  static final int BIG_ENDIAN_FLAG_MASK       = 1;
  static final int READ_ONLY_FLAG_MASK        = 2;
  static final int EMPTY_FLAG_MASK            = 4;
  static final int COMPACT_FLAG_MASK          = 8;
  static final int ORDERED_FLAG_MASK          = 16;

  static final boolean NATIVE_ORDER_IS_BIG_ENDIAN  =
      (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);

  /**
   * Default K for about 1.7% normalized rank accuracy
   */
  static final int DEFAULT_K = 128;

  // ###### TO HERE.

  // STRINGS
  /**
   * Returns a human readable string summary of the internal state of the given byte array.
   * Used primarily in testing.
   *
   * @param byteArr the given byte array.
   * @param isDoublesSketch flag to indicate that the byte array represents DoublesSketch
   * to output min and max quantiles in the summary
   * @return the summary string.
   */
  static String toString(final byte[] byteArr, final boolean isDoublesSketch) {
    final Memory mem = Memory.wrap(byteArr);
    return toString(mem, isDoublesSketch);
  }

  /**
   * Returns a human readable string summary of the Preamble of the given Memory. If this Memory
   * image is from a DoublesSketch, the MinQuantile and MaxQuantile will also be output.
   * Used primarily in testing.
   *
   * @param mem the given Memory
   * @param isDoublesSketch flag to indicate that the byte array represents DoublesSketch
   * to output min and max quantiles in the summary
   * @return the summary string.
   */
  static String toString(final Memory mem, final boolean isDoublesSketch) {
    return memoryToString(mem, isDoublesSketch);
  }

  private static String memoryToString(final Memory srcMem, final boolean isDoublesSketch) {
    final int preLongs = extractPreLongs(srcMem); //either 1 or 2
    final int serVer = extractSerVer(srcMem);
    final int familyID = extractFamilyID(srcMem);
    final String famName = idToFamily(familyID).toString();
    final int flags = extractFlags(srcMem);
    final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    final String nativeOrder = ByteOrder.nativeOrder().toString();
    final boolean readOnly  = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean compact = (flags & COMPACT_FLAG_MASK) > 0;
    final boolean ordered = (flags & ORDERED_FLAG_MASK) > 0;
    final int k = extractK(srcMem);

    final long n = (preLongs == 1) ? 0L : extractN(srcMem);
    double minDouble = Double.NaN;
    double maxDouble = Double.NaN;
    if ((preLongs > 1) && isDoublesSketch) { // preLongs = 2 or 3
      minDouble = extractMinDouble(srcMem);
      maxDouble = extractMaxDouble(srcMem);
    }

    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("### QUANTILES SKETCH PREAMBLE SUMMARY:").append(LS);
    sb.append("Byte  0: Preamble Longs       : ").append(preLongs).append(LS);
    sb.append("Byte  1: Serialization Version: ").append(serVer).append(LS);
    sb.append("Byte  2: Family               : ").append(famName).append(LS);
    sb.append("Byte  3: Flags Field          : ").append(String.format("%02o", flags)).append(LS);
    sb.append("  BIG ENDIAN                  : ").append(bigEndian).append(LS);
    sb.append("  (Native Byte Order)         : ").append(nativeOrder).append(LS);
    sb.append("  READ ONLY                   : ").append(readOnly).append(LS);
    sb.append("  EMPTY                       : ").append(empty).append(LS);
    sb.append("  COMPACT                     : ").append(compact).append(LS);
    sb.append("  ORDERED                     : ").append(ordered).append(LS);
    sb.append("Bytes  4-5  : K               : ").append(k).append(LS);
    if (preLongs == 1) {
      sb.append(" --ABSENT, ASSUMED:").append(LS);
    }
    sb.append("Bytes  8-15 : N                : ").append(n).append(LS);
    if (isDoublesSketch) {
      sb.append("MinDouble                      : ").append(minDouble).append(LS);
      sb.append("MaxDouble                      : ").append(maxDouble).append(LS);
    }
    sb.append("Retained Items                 : ").append(computeRetainedItems(k, n)).append(LS);
    sb.append("Total Bytes                    : ").append(srcMem.getCapacity()).append(LS);
    sb.append("### END SKETCH PREAMBLE SUMMARY").append(LS);
    return sb.toString();
  }

  //@formatter:on
  static int extractPreLongs(final Memory mem) {
    return mem.getByte(PREAMBLE_LONGS_BYTE) & 0XFF;
  }

  static int extractSerVer(final Memory mem) {
    return mem.getByte(SER_VER_BYTE) & 0XFF;
  }

  static int extractFamilyID(final Memory mem) {
    return mem.getByte(FAMILY_BYTE) & 0XFF;
  }

  static int extractFlags(final Memory mem) {
    return mem.getByte(FLAGS_BYTE) & 0XFF;
  }

  static int extractK(final Memory mem) {
    return mem.getShort(K_SHORT) & 0XFFFF;
  }

  static long extractN(final Memory mem) {
    return mem.getLong(N_LONG);
  }

  static double extractMinDouble(final Memory mem) {
    return mem.getDouble(MIN_DOUBLE);
  }

  static double extractMaxDouble(final Memory mem) {
    return mem.getDouble(MAX_DOUBLE);
  }

  static void insertPreLongs(final WritableMemory wmem, final int numPreLongs) {
    wmem.putByte(PREAMBLE_LONGS_BYTE, (byte) numPreLongs);
  }

  static void insertSerVer(final WritableMemory wmem, final int serVer) {
    wmem.putByte(SER_VER_BYTE, (byte) serVer);
  }

  static void insertFamilyID(final WritableMemory wmem, final int famId) {
    wmem.putByte(FAMILY_BYTE, (byte) famId);
  }

  static void insertFlags(final WritableMemory wmem, final int flags) {
    wmem.putByte(FLAGS_BYTE, (byte) flags);
  }

  static void insertK(final WritableMemory wmem, final int k) {
    wmem.putShort(K_SHORT, (short) k);
  }

  static void insertN(final WritableMemory wmem, final long n) {
    wmem.putLong(N_LONG, n);
  }

  static void insertMinDouble(final WritableMemory wmem, final double minDouble) {
    wmem.putDouble(MIN_DOUBLE, minDouble);
  }

  static void insertMaxDouble(final WritableMemory wmem, final double maxDouble) {
    wmem.putDouble(MAX_DOUBLE, maxDouble);
  }
}
