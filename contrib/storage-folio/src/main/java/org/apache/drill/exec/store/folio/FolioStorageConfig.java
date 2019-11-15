package org.apache.drill.exec.store.folio;

import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(FolioStorageConfig.NAME)
public class FolioStorageConfig extends StoragePluginConfig {

  public static final String NAME = "folio";

  private final String url;
  private final String username;
  private final String password;

  @JsonCreator
  public FolioStorageConfig(
      @JsonProperty("url") String url,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password) {
    super();
    this.url = url;
    this.username = username;
    this.password = password;
  }

  public String getUrl() {
    return url;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}