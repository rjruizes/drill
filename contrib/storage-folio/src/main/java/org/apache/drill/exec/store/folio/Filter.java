package org.apache.drill.exec.store.folio;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"asMap"})
public class Filter {
  @JsonIgnore
  private final LinkedHashMap<String, Object> asMap;

  public Filter() {
    asMap = new LinkedHashMap<String, Object>();
  }
  public Filter(final String key, final Object value) {
    asMap = new LinkedHashMap<String, Object>();
    asMap.put(key, value);
  }
  public Object put(final String key, final Object value) {
    return asMap.put(key, value);
  }
  public int getInteger(final Object key, final int defaultValue) {
    return get(key, defaultValue);
  }
  public String getString(final Object key) {
    return (String) get(key);
}
  // @Override
  public Object get(final Object key) {
      return asMap.get(key);
  }
  public <T> T get(final Object key, final Class<T> clazz) {
    return clazz.cast(asMap.get(key));
  }
  @SuppressWarnings("unchecked")
  public <T> T get(final Object key, final T defaultValue) {
    Object value = asMap.get(key);
    return value == null ? defaultValue : (T) value;
  }
  @Override
  public boolean equals(final Object o) {
      if (this == o) {
          return true;
      }
      if (o == null || getClass() != o.getClass()) {
          return false;
      }
      Filter filter = (Filter) o;
      if (!asMap.equals(filter.asMap)) {
          return false;
      }
      return true;
  }

  @Override
  public int hashCode() {
      return asMap.hashCode();
  }
  // @Override
  public Collection<Object> values() {
      return asMap.values();
  }
  // @Override
  public Set<Map.Entry<String, Object>> entrySet() {
      return asMap.entrySet();
  }
  // @Override
  public Set<String> keySet() {
      return asMap.keySet();
  }
  // @Override
  public int size() {
      return asMap.size();
  }
  @Override
  public String toString() {
      return "Filter{"
             + asMap
             + '}';
  }
  public String toCql() {
    String field = (String) asMap.keySet().toArray()[0];
    Filter child = (Filter) asMap.values().toArray()[0];
    String op = (String) child.keySet().toArray()[0];
    String val = (String) child.values().toArray()[0];
    return field + " " + op + " \"" + val + "\"";
  }
}