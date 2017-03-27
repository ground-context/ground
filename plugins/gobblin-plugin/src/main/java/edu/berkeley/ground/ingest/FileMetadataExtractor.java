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

import com.google.common.collect.ImmutableList;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.filebased.FileBasedExtractor;
import gobblin.source.extractor.hadoop.AvroFsHelper;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An extractor that pulls out file metadata.
 */
public class FileMetadataExtractor extends FileBasedExtractor<Schema, GenericRecord> {

  private final String sourceDirectory;

  //creating the schema to store the file metadata
  private static final String SCHEMA_STRING = "{\"namespace\": \"ground.avro\", "
      + "\"type\": \"record\","
      + "\"name\": \"Metadata\", "
      + "\"fields\": [ "
      + "{\"name\": \"name\", \"type\": [\"string\", \"null\"]}, "
      + "{\"name\": \"timeCreated\", \"type\": [\"long\", \"null\"]},"
      + "{\"name\": \"length\", \"type\": [\"long\", \"null\"]},"
      + "{\"name\": \"modificationTime\", \"type\": [\"long\", \"null\"]},"
      + "{\"name\": \"owner\", \"type\": [\"string\", \"null\"]} "

      + "]"
      + "}";

  private static Schema OUTPUT_SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
  private final FileSystem fs;

  /**
   * Extract the metadata for each file.
   *
   * @param workUnitState the unit to extract metadata for
   * @throws IOException error with extracting file metadata
   */
  public FileMetadataExtractor(WorkUnitState workUnitState) throws IOException {
    super(workUnitState, new AvroFsHelper(workUnitState));
    Properties props = workUnitState.getProperties();
    Config config = ConfigFactory.parseProperties(props);
    this.sourceDirectory = config.getString(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY);
    fs = FileSystem.get(URI.create(sourceDirectory), new Configuration());
  }

  @Override
  public Iterator<GenericRecord> downloadFile(String file) throws IOException {

    Path path = new Path(file);
    FileStatus[] statuses = fs.listStatus(path);

    //creating the GenericRecord to store the metadata
    GenericRecord record = new GenericData.Record(OUTPUT_SCHEMA);
    record.put("name", statuses[0].getPath().toString());
    record.put("timeCreated", statuses[0].getAccessTime());
    record.put("length", statuses[0].getLen());
    record.put("modificationTime", statuses[0].getModificationTime());
    record.put("owner", statuses[0].getOwner());

    return ImmutableList.of(record).iterator();

  }

  @Override
  public Schema getSchema() {
    return OUTPUT_SCHEMA;
  }

}
