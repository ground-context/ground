/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.plugins.hive.util;

import com.google.gson.Gson;

public class JsonUtil {

  private JsonUtil() {
  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    Gson gson = new Gson();
    return gson.fromJson(json.replace("\\", ""), clazz);
  }

  public static String toJson(Object object) {
    Gson gson = new Gson();
    return gson.toJson(object);
  }
}
