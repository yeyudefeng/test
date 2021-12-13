package com.clife.udf;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class DingDingClient {
    HttpClient httpclient = HttpClients.createDefault();

    public void send(String webhook, Object message) throws IOException
    {
        HttpPost httppost = new HttpPost(webhook);
        httppost.addHeader("Content-Type", "application/json; charset=utf-8");
        StringEntity se = new StringEntity(message.toString(), "utf-8");
        httppost.setEntity(se);


//        System.out.println("qy start send ...");
        HttpResponse response = this.httpclient.execute(httppost);
        if (response.getStatusLine().getStatusCode() == 200) {
            String result = EntityUtils.toString(response.getEntity());

        }

    }
}