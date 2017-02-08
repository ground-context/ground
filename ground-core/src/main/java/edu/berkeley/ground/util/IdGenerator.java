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

public class IdGenerator {
  private long prefix;
  private long versionCounter;
  private long successorCounter;

  public IdGenerator(int machineId, int numMachines) {
    long machineBits = 1;
    long fence = 2;

    while (fence < numMachines) {
      fence = fence * 2;
      machineBits++;
    }

    this.prefix = machineId << (64 - machineBits);

    // NOTE: Do not change this. The version counter is set to start a 1 because 0 is the default empty version.
    this.versionCounter = 1;
    this.successorCounter = 0;
  }

  public synchronized long generateVersionId() {
    return prefix | this.versionCounter++;
  }


  public synchronized long generateSuccessorId() {
    return prefix | this.successorCounter++;
  }
}
