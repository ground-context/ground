package edu.berkeley.ground.ingest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.gson.JsonElement;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.Extract.TableType;

public class WikiSource extends AbstractSource<String, JsonElement>{
  
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    Extract extract = createExtract(TableType.SNAPSHOT_ONLY,
        state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY), "WikipediaOutput");

    WorkUnit workUnit = WorkUnit.create(extract);
    return Arrays.asList(workUnit);
  }

  @Override
  public Extractor<String, JsonElement> getExtractor(WorkUnitState state) throws IOException {
    return new WikiExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    //nothing to do
  }

}
