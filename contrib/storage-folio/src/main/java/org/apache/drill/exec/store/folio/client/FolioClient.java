package org.apache.drill.exec.store.folio.client;

import java.io.IOException;

import org.apache.drill.exec.store.folio.client.Login;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class FolioClient {
    public CloseableHttpClient httpclient;
    public String okapiUrl;
    private String tenant;
    private String token;

    public FolioClient(String okapiUrl, String tenant, String user, String pass) throws ClientProtocolException, IOException {
        this.httpclient = HttpClients.createDefault();
        this.okapiUrl = okapiUrl;
        this.tenant = tenant;
        this.token = Login.login(okapiUrl, tenant, user, pass);
    }

    private ResponseHandler<String> responseHandler() {
        return new ResponseHandler<String>() {
            @Override
            public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                int status = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();
                String resp = entity != null ? EntityUtils.toString(entity) : null;
                if (status >= 200 && status < 300) {
                    return resp;
                } else {
                    System.out.println("Response from server: " + resp);
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }
            }
        };
    }

    private HttpUriRequest authenticatedRequest(String method, String path) {
        return RequestBuilder.create(method)
            .setUri(okapiUrl + path)
            .setHeader("Accept", "application/json")
            .setHeader("X-Okapi-Tenant", tenant)
            .setHeader("X-Okapi-Token", token)
            .build();
    }

    public String get(String path) throws ClientProtocolException, IOException {
        HttpUriRequest request = authenticatedRequest("GET", path);
        return httpclient.execute(request, responseHandler());
    }

    public String post(String path) throws ClientProtocolException, IOException {
        HttpUriRequest request = authenticatedRequest("POST", path);
        return httpclient.execute(request, responseHandler());
    }

    // public final static void main(String[] args) throws Exception {
    //     FolioClient fc = new FolioClient();
    //     System.out.println(fc.get("/inventory/instances?limit=30&query=%28holdingsRecords.permanentLocationId%3D%22fcd64ce1-6995-48f0-840e-89ffa2288371%22%29%20sortby%20title"));
    // }

}
