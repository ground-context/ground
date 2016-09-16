/*
  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
  in compliance with the License. You may obtain a copy of the License at

  <p>http://www.apache.org/licenses/LICENSE-2.0

  <p>Unless required by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  express or implied. See the License for the specific language governing permissions and
  limitations under the License.
 */
package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.berkeley.ground.api.models.github.GithubWebhook;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.KafkaProduce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/github")
@Produces(MediaType.APPLICATION_JSON)
public class GithubWebhookResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(GithubWebhookResource.class);

    private String kafkaHost;
    private String kafkaPort;

    public GithubWebhookResource(String kafkaHost, String kafkaPort) {
        this.kafkaHost = kafkaHost;
        this.kafkaPort = kafkaPort;
  }

  @POST
  @Timed
  @Path("/webhook")
  public GithubWebhook makeGithubWebhook(@Valid GithubWebhook githubWebhook)
      throws GroundException {
    LOGGER.info("Receiving github webhook");

    GithubWebhook ghwh =
        new GithubWebhook(
            githubWebhook.getRef(),
            githubWebhook.getBefore(),
            githubWebhook.getAfter(),
            githubWebhook.getCreated(),
            githubWebhook.getDeleted(),
            githubWebhook.getForced(),
            githubWebhook.getBaseRef(),
            githubWebhook.getCompare(),
            githubWebhook.getCommits(),
            githubWebhook.getHeadCommit(),
            githubWebhook.getRepository(),
            githubWebhook.getPusher(),
            githubWebhook.getSender());

    ObjectMapper mapper = new ObjectMapper();
    String json = "";
    try {
      json = mapper.writeValueAsString(ghwh);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }

    KafkaProduce.push(kafkaHost, kafkaPort, "github", ghwh.getRepository().getName(), json);

    return ghwh;
  }
}
