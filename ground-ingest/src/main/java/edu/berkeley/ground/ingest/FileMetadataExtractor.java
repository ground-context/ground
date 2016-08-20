/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package edu.berkeley.ground.ingest;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.filebased.FileBasedExtractor;
import gobblin.source.extractor.hadoop.AvroFsHelper;


/**
 * An extractor that pulls out file metadata.
 */
public class FileMetadataExtractor extends FileBasedExtractor<Schema, GenericRecord> {

  private static final String SCHEMA_STRING = "{\"namespace\": \"example.avro\", "
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

  public FileMetadataExtractor(WorkUnitState workUnitState)
      throws IOException {
    super(workUnitState, new AvroFsHelper(workUnitState));
    String writerFsURI = "file:///";
    fs = FileSystem.get(URI.create(writerFsURI),new Configuration());
  }

  @Override
  public Iterator<GenericRecord> downloadFile(String file) throws IOException {

    Path path = new Path(file);
    FileStatus[] statuses = fs.listStatus(path);

    GenericRecord record = new GenericData.Record(OUTPUT_SCHEMA);
    record.put("name", statuses[0].getPath().toString());
    record.put("timeCreated", statuses[0].getAccessTime());
    record.put("length", statuses[0].getLen());
    record.put("modificationTime", statuses[0].getModificationTime());
    record.put("owner", statuses[0].getOwner().toString());

    return ImmutableList.of(record).iterator();
  }

  @Override
  public Schema getSchema() {
    return OUTPUT_SCHEMA;
  }

}
