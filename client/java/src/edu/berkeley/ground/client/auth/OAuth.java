package edu.berkeley.ground.client.auth;

import edu.berkeley.ground.client.Pair;

import java.util.Map;
import java.util.List;

@javax.annotation.Generated(value = "edu.berkeley.ground.codegen.languages.JavaClientCodegen", date = "2017-07-24T17:06:34.249-07:00")
public class OAuth implements Authentication {
  private String accessToken;

  public String getAccessToken() {
    return accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  @Override
  public void applyToParams(List<Pair> queryParams, Map<String, String> headerParams) {
    if (accessToken != null) {
      headerParams.put("Authorization", "Bearer " + accessToken);
    }
  }
}
