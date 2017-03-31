package edu.berkeley.ground.util;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nipun.ramk on 3/6/17.
 */
public class ElasticSearch {
  public static Node node;
  public static Client client;
  public static String clusterName = "groundtest";
  public static String typeName;
  public static boolean bool = true;

  public static boolean connectElasticSearch(String table) {
//    System.out.println("IN FUNCTION CONNECT");
    typeName = table;
    try {
      node = nodeBuilder().clusterName(clusterName).node();
      client = node.client();
    } catch (Exception e) {
//      System.out.println("ERROR: " + e.getMessage());
      return false;
    }
    return !node.isClosed();

  }

  public static boolean insertElasticSearch(Tag tag, String table) {
//    System.out.println("INSERTION STARTED");
    boolean connected = connectElasticSearch(table);
    if (!connected) {
      System.out.println("CONNECTION FAILED");
    } else {
      System.out.println("SUCCESSFULLY CONNECTED");
    }
    if (bool) {
      DeleteIndexResponse deleteResponse = client.admin().indices().delete(new DeleteIndexRequest("groundtest")).actionGet();
      bool = false;
    }


    ObjectMapper mapper = new ObjectMapper();

    try {
      String json = mapper.writeValueAsString(tag);
//      System.out.println("JSON STRING: " + json);
      IndexResponse response = client.prepareIndex(clusterName, typeName, Long.toString(tag.getId()))
        .setSource(json).get();
//      System.out.println("CREATED RESPONSE: " + response.isCreated());
      return response.isCreated();

    } catch (JsonProcessingException e) {
      return false;
    }



  }

  public static List<Long> getSearchResponse(String type, String searchQuery) {
    System.out.println(searchQuery);
//    SearchResponse response = client.search(Requests.searchRequest("ground").source(searchQuery)).actionGet();

    SearchResponse response = client.prepareSearch().setTypes(type).setQuery(QueryBuilders.matchQuery("key", searchQuery)).get();
//    SearchResponse response = client.prepareSearch().get();
    System.out.println(response.toString());
    SearchHit[] hits = response.getHits().getHits();

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
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println(tagIds);
    return tagIds;
  }

  public static void main(String[] args) {


//    connectElasticSearch();
    Tag tag1 = new Tag(1, "Test1", "Value1", GroundType.STRING);
    Tag tag2 = new Tag(2, "Test1", "Value2", GroundType.STRING);
    Tag tag3 = new Tag(3, "Test3", "Value3", GroundType.STRING);
    Tag tag4 = new Tag(4, "Test1", "Value4", GroundType.STRING);
    ObjectMapper mapper = new ObjectMapper();


    boolean response1 = insertElasticSearch(tag1, "rich_version");
    boolean response2 = insertElasticSearch(tag2, "rich_version");
    boolean response3 = insertElasticSearch(tag3, "item");
    boolean response4 = insertElasticSearch(tag4, "item");
    if (response1) {
      System.out.println("INSERTED FIRST ITEM");
    } else {
      System.out.println("FAILED TO INSERT ITEM 1");
    }
    if (response2) {
      System.out.println("INSERTED SECOND ITEM");
    } else {
      System.out.println("FAILED TO INSERT ITEM 2");
    }

    if (response3) {
      System.out.println("INSERTED THIRD ITEM");
    } else {
      System.out.println("FAILED TO INSERT ITEM 3");
    }

    if (response4) {
      System.out.println("INSERTED FOURTH ITEM");
    } else {
      System.out.println("FAILED TO INSERT ITEM 4");
    }
    try {
      String json = mapper.writeValueAsString(tag1);
      getSearchResponse("rich_version", "Test1");
//      String result = getSearchResponse(json);
//      System.out.println(result);
      System.out.println("FINISHED SEARCH");
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }





    client.admin().indices().prepareRefresh().execute().actionGet();
    node.close();
    client.close();

  }

}
