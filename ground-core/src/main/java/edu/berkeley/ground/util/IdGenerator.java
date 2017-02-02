/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class IdGenerator {
  private static int counter = 0;

  private static int numBytes = 20;
  private static final String SEED = byteArrayToString(new SecureRandom().generateSeed(numBytes));

  public static String generateId(String baseId) {

    return SHA1(SEED + counter++ + baseId);
  }

  private static String SHA1(String str) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA1");
      return byteArrayToString(md.digest(str.getBytes()));
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("FATAL ERROR: No SHA1 algorithm found in MessageDigest.");
    }
  }

  private static String byteArrayToString(byte[] array) {
    StringBuilder sb = new StringBuilder();

    for (byte b : array) {
      sb.append(Integer.toString(b & 0xff + 0x100, 16).substring(1));
    }

    return sb.toString();
  }
}
