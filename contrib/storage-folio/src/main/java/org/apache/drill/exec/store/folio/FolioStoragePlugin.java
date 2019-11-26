package org.apache.drill.exec.store.folio;

import java.io.IOException;

import org.apache.drill.exec.store.folio.client.FolioClient;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.folio.schema.FolioSchemaFactory;
import org.apache.http.client.ClientProtocolException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FolioStoragePlugin extends AbstractStoragePlugin {
  private static final Logger logger = LoggerFactory.getLogger(FolioStoragePlugin.class);
  private final FolioStorageConfig config;
  private final FolioSchemaFactory schemaFactory;

  private final FolioClient client;

  public FolioStoragePlugin(FolioStorageConfig config, DrillbitContext context, String name)
      throws ClientProtocolException, IOException {
    super(context, name);
    this.config = config;
    this.schemaFactory = new FolioSchemaFactory(this, name);
    this.client = new FolioClient();
    System.out.println("Start folio plugin");
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    this.schemaFactory.registerSchemas(schemaConfig, parent);
  }

  public FolioClient getClient() {
    return client;
  }

  @Override
  public FolioStorageConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public FolioGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    FolioScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<FolioScanSpec>() {});
    return new FolioGroupScan(this, scanSpec, null);
  }
}