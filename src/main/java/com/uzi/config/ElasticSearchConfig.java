package com.uzi.config;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ScriptLanguage;
import co.elastic.clients.elasticsearch.core.GetScriptResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.uzi.constant.Constants;
import lombok.Data;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Data
@Configuration
@ConfigurationProperties(prefix = "elasticsearch")
public class ElasticSearchConfig {

    private String host;
    private Integer port;
    private Integer ingesterMaxOperation;
    private Integer ingesterFlushInterval;
    private Integer ingesterConcurrentThreadsNum;

    @Autowired
    private RestClientTransport restClientTransport;

    @Bean
    public ElasticsearchAsyncClient elasticsearchAsyncClient() {
        return new ElasticsearchAsyncClient(getElasticsearchTransport());
    }

    @Bean
    public ElasticsearchClient elasticsearchClient() {
        ElasticsearchClient client = new ElasticsearchClient(restClientTransport);
        putScript(client);
        return client;
    }

    private ElasticsearchTransport getElasticsearchTransport() {
        RestClient restClient = RestClient.builder(new HttpHost(host, port)).build();
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    private void putScript(ElasticsearchClient client) {
        GetScriptResponse response;
        try {
            response = client.getScript(r -> r.id(Constants.QUERY_SCRIPT));
            if (response.found()) {
                return;
            }
            client.putScript(r -> r.id(Constants.QUERY_SCRIPT)
            .script(s -> s.lang(ScriptLanguage.Mustache)
            .source(Constants.SOURCE_VALUE)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
