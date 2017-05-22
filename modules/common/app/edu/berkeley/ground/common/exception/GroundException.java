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
package edu.berkeley.ground.common.exception;

public class GroundException extends Exception {

  private static final long serialVersionUID = 1L;

  private final String message;
  private final ExceptionType exceptionType;

  public enum ExceptionType {
    DB("Database Exception:", "%s"),
    ITEM_NOT_FOUND("GroundItemNotFoundException", "No %s \'%s\' found."),
    VERSION_NOT_FOUND("GroundVersionNotFoundException", "No %s \'%s\' found."),
    ITEM_ALREADY_EXISTS("GroundItemAlreadyExistsException", "%s %s already exists."),
    OTHER("GroundException", "%s");

    String name;
    String description;

    ExceptionType(String name, String description) {
      this.name = name;
      this.description = description;
    }

    public String format(String... values) {
      return String.format(this.description, values);
    }
  }

  public GroundException(ExceptionType exceptionType, String... values) {
    this.exceptionType = exceptionType;
    this.message = this.exceptionType.format(values);
  }

  public GroundException(Exception exception) {
    this.exceptionType = ExceptionType.OTHER;
    this.message = this.exceptionType.format(exception.getMessage());
  }

  @Override
  public String getMessage() {
    return this.message;
  }

  public ExceptionType getExceptionType() {
    return this.exceptionType;
  }
}
