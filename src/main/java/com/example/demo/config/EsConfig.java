package com.example.demo.config;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
@ConfigurationProperties(prefix = "elasticsearch")
public class EsConfig {

    @Value("${elasticsearch.host}")
    private String host;

    @Value("${elasticsearch.port}")
    private Integer port;

    @Bean
    public RestClientBuilder restClientBuilder() {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(host, port, "http"));
        restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setKeepAliveStrategy(((httpResponse, httpContext) ->
                        TimeUnit.MINUTES.toMillis(1))));
        return restClientBuilder;
    }

    @Bean
    public RestClient restClient(@Qualifier(value = "restClientBuilder") RestClientBuilder restClientBuilder) {
        return  restClientBuilder.build();
    }

    @Bean
    public RestHighLevelClient restHighLevelClient(@Qualifier(value = "restClientBuilder") RestClientBuilder restClientBuilder) {
        return new RestHighLevelClient(restClientBuilder);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
}
