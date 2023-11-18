package com.uzi.utils;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.json.JsonData;
import com.uzi.constant.Constants;
import com.uzi.entity.User;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ToString
public class EsUtil<T> {

    private final Logger logger = LoggerFactory.getLogger(EsUtil.class);

    @Autowired
    private ElasticsearchAsyncClient client;

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    public boolean existsIndex(String indexName) throws Exception {
        return client.indices().exists(r -> r.index(indexName)).get().value();
    }

    public boolean createIndex(String indexName) throws Exception {
        CreateIndexRequest request = CreateIndexRequest.of(b -> b.index(indexName));
        return client.indices().create(request).get().acknowledged();
    }

    public boolean deleteIndex(String indexName) throws Exception {
        DeleteIndexRequest request = DeleteIndexRequest.of(b -> b.index(indexName));
        return client.indices().delete(request).get().acknowledged();
    }

    /**
     * 添加文档记录
     *
     * @param indexName
     * @param id
     * @param t
     */
    public void addDoc(String indexName, String id, T t) {
        IndexRequest<Object> request = IndexRequest.of(b -> b.index(indexName).id(id).document(t));
        client.index(request);
    }

    public boolean getDoc(String indexName, String id) throws Exception {
        GetRequest request = GetRequest.of(b -> b.index(indexName).id(id));
        GetResponse<User> response = client.get(request, User.class).get();
        System.out.println("result: " + response.source());
        return response.found();
    }

    public boolean bulkAdd(String indexName, List<T> list) throws Exception {
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (T t : list) {
            br.operations(op -> op.index(idx -> idx.index(indexName).document(t)));
        }
        BulkResponse result = elasticsearchClient.bulk(br.build());
        if (result.errors()) {
            logger.error("Bulk had errors");
            for (BulkResponseItem item : result.items()) {
                if (item.error() != null){
                    logger.error(item.error().reason());
                }
            }
            return false;
        }
        return true;
    }

    /**
     * 根据某字段来搜索
     *
     * @param indexName
     * @param searchText 要搜索的关键字
     * @return
     * @throws Exception
     */
    public boolean search(String indexName, String searchText) throws Exception {
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(indexName)
                .query(q -> q
                        .match(t -> t
                                .field("name")
                                .query(searchText))));
        SearchResponse<User> response = elasticsearchClient.search(searchRequest, User.class);

        TotalHits total = response.hits().total();
        boolean isExactResult = total.relation() == TotalHitsRelation.Eq;
        if (isExactResult) {
            logger.info("There are " + total.value() + " results");
        } else {
            logger.info("There are more than " + total.value() + " results");
        }

        List<Hit<User>> hits = response.hits().hits();
        for (Hit<User> hit : hits) {
            User user = hit.source();
            logger.info("Found user " + user + ", source " + hit.source());
        }
        return true;
    }

    public boolean searchTemplate(String indexName, String searchText) throws Exception {
        SearchTemplateResponse<User> response = elasticsearchClient.searchTemplate(r -> r
                .index(indexName)
                .id(Constants.QUERY_SCRIPT)
                .params("field", JsonData.of("name"))
                .params("value", JsonData.of(searchText)), User.class);
        if (response.terminatedEarly() != null && response.terminatedEarly()) {
            return false;
        }
        List<Hit<User>> hits = response.hits().hits();
        for (Hit<User> hit : hits) {
            User user = hit.source();
            logger.info("Found user " + user + ", source " + hit.source());
        }
        return true;
    }
}
