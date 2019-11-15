package org.apache.drill.exec.store.folio;

import java.io.IOException;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.folio.schema.FolioSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FolioStoragePlugin extends AbstractStoragePlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FolioStoragePlugin.class);
  private final FolioStorageConfig config;
  private final FolioSchemaFactory schemaFactory;
  // private final DataSource source;
  
  public FolioStoragePlugin(FolioStorageConfig config, DrillbitContext context, String name) {
    super(context, name);
    this.config = config;
    // BasicDataSource source = new BasicDataSource();
    // source.setDriverClassName(config.getDriver());
    // source.setUrl(config.getUrl());

    // if (config.getUsername() != null) {
    //   source.setUsername(config.getUsername());
    // }

    // if (config.getPassword() != null) {
    //   source.setPassword(config.getPassword());
    // }

    // this.source = source;
    this.schemaFactory = new FolioSchemaFactory(this, name);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    this.schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public FolioStorageConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }
}