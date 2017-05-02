/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.lib.exception;

public class GroundException extends Exception {
  private static final long serialVersionUID = 1L;
  private final String message;

  public enum exceptionType {
    DB,
    ITEM_NOT_FOUND,
    ITEM_EXISTS;
  }

  public GroundException(String message) {
    this.message = message;
  }

  public GroundException(Exception exception) {
    this.message = exception.getClass() + ":" + exception.getMessage();
  }

  @Override
  public String getMessage() {
    return this.message;
  }
}
