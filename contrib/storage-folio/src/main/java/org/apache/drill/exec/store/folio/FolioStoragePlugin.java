package org.apache.drill.exec.store.folio;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Set;

import org.apache.drill.exec.store.folio.client.FolioClient;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.folio.schema.FolioSchemaFactory;
import org.apache.http.client.ClientProtocolException;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FolioStoragePlugin extends AbstractStoragePlugin {
  // private static final Logger logger = LoggerFactory.getLogger(FolioStoragePlugin.class);
  private final FolioStorageConfig config;
  private final FolioSchemaFactory schemaFactory;

  private FolioClient client;

  public FolioStoragePlugin(FolioStorageConfig config, DrillbitContext context, String name) {
    super(context, name);
    this.config = config;
    this.schemaFactory = new FolioSchemaFactory(this, name);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    this.schemaFactory.registerSchemas(schemaConfig, parent);
  }

  public FolioClient getClient() throws ClientProtocolException, IOException, URISyntaxException {
    if(client == null) {
      this.client = new FolioClient(config.getUrl(), config.getTenant(), config.getUsername(), config.getPassword());
    }
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

  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return ImmutableSet.of(FolioPushDownFilterForScan.INSTANCE);
  }
}