package edu.berkeley.ground.ingest;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.filebased.FileBasedSource;
import gobblin.source.extractor.hadoop.HadoopFsHelper;
import gobblin.util.HadoopUtils;


/**
 * A Source that extracts metadata from a file system.
 */
public class FileMetadataSource extends FileBasedSource<Schema, GenericRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileMetadataSource.class);
  private final String splitPattern = ":::";
  private String path;

  @Override
  public void initFileSystemHelper(State state)
      throws FileBasedHelperException {
    this.fsHelper = new HadoopFsHelper(state, HadoopUtils.newConfiguration());
    this.fsHelper.connect();
    this.path = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY);
  }

  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state)
      throws IOException {
    return new FileMetadataExtractor(state);
  }

  @Override
  public List<String> getcurrentFsSnapshot(State state) {
    List<String> results = Lists.newArrayList();

    try {
      LOGGER.info("Running ls command with input " + path);
      results = this.fsHelper.ls(path);
      for (int i = 0; i < results.size(); i++) {
        String filePath = results.get(i);
        results.set(i, filePath + this.splitPattern + this.fsHelper.getFileMTime(filePath));
      }
    } catch (FileBasedHelperException e) {
      LOGGER.error("Not able to fetch the filename/file modified time to " + e.getMessage() + " will not pull any files",
          e);
    }
    return results;

  }
}
