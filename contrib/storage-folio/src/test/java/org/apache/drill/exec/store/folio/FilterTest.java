package org.apache.drill.exec.store.folio;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FilterTest {

  @Test
  public void equalityTest() throws Exception {
    String id = "758258bc-ecc1-41b8-abca-f7b610822ffd";
    Filter filter = new Filter("id",
      new Filter("=", id)
    );
    assertEquals("id = \""+id+"\"", filter.toCql());
  }
}