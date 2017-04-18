package edu.berkeley.ground.util;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


public class ElasticSearch {
  private static Node node;
  private static Client client;
  private static String clusterName = "groundv1";



  public static boolean connectElasticSearch() throws GroundException {
    try {
      node = nodeBuilder().clusterName(clusterName).node();
      client = node.client();
    } catch (Exception e) {
      throw new GroundException("ElasticSearch encountered an error while trying to connect");
    }
    return !node.isClosed();

  }

  public static boolean insertElasticSearch(Tag tag, String table) throws GroundException {


    ObjectMapper mapper = new ObjectMapper();

    try {
      String json = mapper.writeValueAsString(tag);
      IndexResponse response = client.prepareIndex(clusterName, table, Long.toString(tag.getId()))
        .setSource(json).get();
      client.admin().indices().prepareRefresh().execute().actionGet(); // need to refresh index with new inserted item

      return response.isCreated();

    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new GroundException("ObjectMapper failed to parse Tag object");
    }



  }

  public static List<Long> getSearchResponse(String type, String searchQuery) throws GroundException {
    SearchResponse response = client.prepareSearch().setTypes(type).setQuery(QueryBuilders.matchQuery("key", searchQuery)).get();
    SearchHit[] hits = response.getHits().hits();

    ObjectMapper mapper = new ObjectMapper();
    List<Long> tagIds = new ArrayList<>();
    try {
      for (SearchHit hit : hits) {
        String source = hit.getSourceAsString();
        Tag tag = mapper.readValue(source, Tag.class);
        tagIds.add(tag.getId());

      }
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw new GroundException("ObjectMapper failed to parse json string into Tag object");
    } catch (JsonMappingException e) {
      e.printStackTrace();
      throw new GroundException("ObjectMapper failed to map json string into Tag object");
    } catch (IOException e) {
      e.printStackTrace();
      throw new GroundException("ObjectMapper detected an IOException");
    }
    return tagIds;
  }

  public static void closeElasticSearch() {
    node.close();
    client.close();
  }
}
