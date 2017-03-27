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

package edu.berkeley.ground;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

public class GroundServerConfiguration extends Configuration {

  @JsonProperty("swagger")
  public SwaggerBundleConfiguration swaggerBundleConfiguration;

  @NotEmpty
  private String dbType;

  @NotEmpty
  private String dbName;

  @NotEmpty
  private String dbHost;

  @NotNull
  private Integer dbPort;

  @NotEmpty
  private String dbUser;

  @NotEmpty
  private String dbPassword;

  @NotEmpty
  private String kafkaHost;

  @NotEmpty
  private String kafkaPort;

  @NotNull
  private Integer numMachines;

  @NotNull
  private Integer machineId;

  @JsonProperty
  public String getDbType() {
    return this.dbType;
  }

  @JsonProperty
  public void setDbType(String dbType) {
    this.dbType = dbType;
  }

  @JsonProperty
  public String getDbName() {
    return this.dbName;
  }

  @JsonProperty
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  @JsonProperty
  public String getDbHost() {
    return this.dbHost;
  }

  @JsonProperty
  public void setDbHost(String dbHost) {
    this.dbHost = dbHost;
  }

  @JsonProperty
  public Integer getDbPort() {
    return this.dbPort;
  }

  @JsonProperty
  public void setDbPort(Integer dbPort) {
    this.dbPort = dbPort;
  }

  @JsonProperty
  public String getDbUser() {
    return this.dbUser;
  }

  @JsonProperty
  public void setDbUser(String dbUser) {
    this.dbUser = dbUser;
  }

  @JsonProperty
  public String getDbPassword() {
    return this.dbPassword;
  }

  @JsonProperty
  public void setDbPassword(String dbPassword) {
    this.dbPassword = dbPassword;
  }

  @JsonProperty
  public String getKafkaHost() {
    return this.kafkaHost;
  }

  @JsonProperty
  public void setKafkaHost(String kafkaHost) {
    this.kafkaHost = kafkaHost;
  }

  @JsonProperty
  public String getKafkaPort() {
    return this.kafkaPort;
  }

  @JsonProperty
  public void setKafkaPort(String kafkaPort) {
    this.kafkaPort = kafkaPort;
  }

  @JsonProperty
  public Integer getNumMachines() {
    return this.numMachines;
  }

  @JsonProperty
  public void setNumMachines(Integer numMachines) {
    this.numMachines = numMachines;
  }

  @JsonProperty
  public Integer getMachineId() {
    return this.machineId;
  }

  @JsonProperty
  public void setMachineId(Integer machineId) {
    this.machineId = machineId;
  }
}