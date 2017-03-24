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

package edu.berkeley.ground.ingest;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.ConfigFactory;

import java.util.Properties;

import gobblin.configuration.State;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

import com.typesafe.config.Config;


public class GroundDataWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {

  @Override
  public DataWriter<GenericRecord> build() throws IOException {

    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    Config writerConfig = ConfigFactory.parseProperties(taskProps);
    return new GroundWriter<>(writerConfig);
  }


}
