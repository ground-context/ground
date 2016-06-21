package edu.berkeley.ground.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.elasticsearch.managed.ManagedEsClient;
import io.dropwizard.jackson.Jackson;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class ElasticSearchClient {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();
    private ManagedEsClient esClient;

    public ElasticSearchClient(ManagedEsClient esClient) {
        this.esClient = esClient;
    }

    public void indexTags(String id, Map<String, Tag> tags) throws GroundException {
        XContentBuilder jsonBuilder;
        try {
            jsonBuilder = jsonBuilder().startObject();

            for (Map.Entry<String, Tag> entry : tags.entrySet()) {
                jsonBuilder.field(entry.getKey(), MAPPER.writeValueAsString(entry.getValue()));
            }

            jsonBuilder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
            throw new GroundException(e);
        }

        this.esClient.getClient().prepareIndex("ground", "tag", id).setSource(jsonBuilder).get();
    }
}
