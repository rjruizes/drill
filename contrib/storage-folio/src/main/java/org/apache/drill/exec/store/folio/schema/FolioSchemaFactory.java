package org.apache.drill.exec.store.folio.schema;

import java.io.IOException;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.folio.FolioStoragePlugin;

public class FolioSchemaFactory extends AbstractSchemaFactory {

  private final FolioStoragePlugin plugin;

  public FolioSchemaFactory(FolioStoragePlugin plugin, String schemaName) {
    super(schemaName);
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    FolioSchema schema = new FolioSchema(plugin, getName());
    SchemaPlus hPlus = parent.add(getName(), schema);
    schema.setHolder(hPlus);
  }

}
