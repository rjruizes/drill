package org.apache.drill.exec.store.folio.raml;

import java.util.ArrayList;

// Analogous to a column: has a name and a type
public class ApiField {
  private String name;
  private String fieldType;
  private ArrayList<ApiField> children;

  public ApiField(String name, String fieldType) {
    this.name = name;
    this.fieldType = fieldType;
  }
  public ApiField(String name, String fieldType, ArrayList<ApiField> children) {
    this.name = name;
    this.fieldType = fieldType;
    this.children = children;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public String getType() {
    return fieldType;
  }
  public void setFieldType(String fieldType) {
    this.fieldType = fieldType;
  }
  public ArrayList<ApiField> getChildren() {
    return children;
  }
}