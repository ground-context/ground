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

package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.KafkaProduce;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.validation.Valid;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/kafka")
@Api(value = "/kafka", description = "Interact with ground's kafka cluster")
@Produces(MediaType.APPLICATION_JSON)
public class KafkaResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResource.class);

  private final String kafkaHost;
  private final String kafkaPort;

  public KafkaResource(String kafkaHost, String kafkaPort) {
    this.kafkaHost = kafkaHost;
    this.kafkaPort = kafkaPort;
  }

  /**
   * Send a message to kafka.
   *
   * @param value the value of the message
   * @param topic the topic to send the message to
   * @param key the message key
   * @return the sent value
   * @throws GroundException an exception while sending the message
   */
  @POST
  @Timed
  @ApiOperation(value = "Send kafka message")
  @ApiResponses(value = {@ApiResponse(code = 405, message = "Invalid input")})
  @Path("/")
  public String sendKafkaMessage(
      @Valid String value,
      @ApiParam(value = "Topic to push to", required = true) @QueryParam("topic") String topic,
      @ApiParam(value = "Key of message", required = true) @QueryParam("key") String key)
      throws GroundException {

    LOGGER.info("Receiving kafka publish");
    KafkaProduce.push(kafkaHost, kafkaPort, topic, key, value);
    return value;
  }
}
