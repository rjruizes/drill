package org.apache.drill.exec.store.folio.client;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class Login {
  public static String login(String okapiUrl, String tenant, String user, String pass) throws ClientProtocolException, IOException {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    
    HttpPost httppost = new HttpPost(okapiUrl + "/bl-users/login");
    String inputJson = "{\n" +
    "  \"username\": \""+user+"\",\n" +
    "  \"password\": \""+pass+"\"\n" +
    "}";

    StringEntity stringEntity = new StringEntity(inputJson);
    httppost.setEntity(stringEntity);
    httppost.setHeader("Accept", "application/json");
    httppost.setHeader("Content-Type", "application/json");
    httppost.setHeader("X-Okapi-Tenant", tenant);
    
    return httpclient.execute(httppost, new ResponseHandler<String>() {
      @Override
      public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
        int status = response.getStatusLine().getStatusCode();
        Header tenantHeader = response.getFirstHeader("x-okapi-token"); // The only header that's lowercase
        HttpEntity entity = response.getEntity();
        String resp = entity != null ? EntityUtils.toString(entity) : null;
        if (status >= 200 && status < 300 && tenantHeader != null) {
          return tenantHeader.getValue();
        } else {
          System.out.println("Response from server: " + resp);
          throw new ClientProtocolException("Unexpected response status: " + status);
        }
      }
    });
  }
}