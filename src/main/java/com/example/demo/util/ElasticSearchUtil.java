package com.example.demo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ElasticSearchUtil {

    private static RestHighLevelClient restHighLevelClient;

    @Autowired
    public void setRestHighLevelClient(@Qualifier(value = "restHighLevelClient") RestHighLevelClient restHighLevelClient) {
        ElasticSearchUtil.restHighLevelClient = restHighLevelClient;
    }

    public void insert(Map<String, Object> data, String indexName) {
        try {
            BulkRequest bulkRequest = new BulkRequest();
            IndexRequest indexRequest = new IndexRequest(indexName, "_doc");
            indexRequest.source(data);
            bulkRequest.add(indexRequest);
            restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
