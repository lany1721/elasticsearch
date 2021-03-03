package cn.zpeace.es;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @author zpeace
 * @date 2021/1/19
 */
@SpringBootTest
public class EsApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    // 创建索引
    @Test
    void createIndex() throws IOException {
        CreateIndexResponse response = client.indices()
                .create(new CreateIndexRequest("es_test"), RequestOptions.DEFAULT);
        System.out.println(response);
    }

    // 获取索引信息 （需索引存在）
    @Test
    void getIndex() throws IOException {
        GetIndexResponse response = client.indices()
                .get(new GetIndexRequest("es_test"), RequestOptions.DEFAULT);
        System.out.println(response.getSettings());
    }

    // 删除索引
    @Test
    void deleteIndex() throws IOException {
        AcknowledgedResponse response = client.indices()
                .delete(new DeleteIndexRequest("es_test"), RequestOptions.DEFAULT);
        System.out.println(response);
    }

    // 创建文档
    @Test
    void createDoc() throws IOException {
        // 索引不存在会自动创建索引
        IndexRequest request = new IndexRequest("es_test");
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    // 获取文档
    @Test
    void getDocument() throws IOException {
        GetRequest request = new GetRequest();
        request.index("es_test");
        request.id("1");
        // 不获取source
//        request.fetchSourceContext(new FetchSourceContext(false));
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    // 更新文档
    @Test
    void updateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest();
        request.index("es_test");
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"zpeace\"," +
                "\"postDate\":\"2020-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        // 需要指定数据类型为Json
        request.doc(jsonString,XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    // 删除文档
    @Test
    void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest();
        request.index("es_test");
        request.id("2");

        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    //批量插入
    @Test
    void bulkCreateDocument() throws IOException {
        BulkRequest request = new BulkRequest();

        for (int i = 0; i < 10; i++) {
            request.add(
                    new IndexRequest().index("es_test")
                    .id(String.valueOf(i))
                    .source("{" +
                            "\"user\"" + ":\"test" +i+"\"," +
                            "\"postDate\":\"2013-01-30\"," +
                            "\"message\":\"trying out Elasticsearch\"" +
                            "}",XContentType.JSON)
            );
        }
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    // 搜索文档
    @Test
    void SearchDocument() throws IOException {
        SearchRequest request = new SearchRequest("es_test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

}
