import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchOperations {

    private static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 8989, "http"))
        );
    }

    public static void createCollection(RestHighLevelClient client, String collectionName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(collectionName);
        request.source("{\n" +
                "  \"settings\": {\n" +
                "    \"number_of_shards\": 1,\n" +
                "    \"number_of_replicas\": 1\n" +
                "  }\n" +
                "}", XContentType.JSON);
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("Collection Name :" + response.index());
    }

    public static void indexData(RestHighLevelClient client, String collectionName, String excludeColumn, String filePath) throws IOException {
        try (CSVParser parser = new CSVParser(new FileReader(filePath), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                Map<String, Object> data = new HashMap<>();
                for (String header : record.toMap().keySet()) {
                    if (!header.equalsIgnoreCase(excludeColumn)) {
                        data.put(header, record.get(header));
                    }
                }
                IndexRequest request = new IndexRequest(collectionName).source(data, XContentType.JSON);
                client.index(request, RequestOptions.DEFAULT);
            }
            System.out.println(excludeColumn+" removed in " + collectionName);
        }
    }

    public static void searchByColumn(RestHighLevelClient client, String collectionName, String columnName, String columnValue) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(columnName, columnValue));
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        for (var hit : response.getHits()) {
            System.out.println("Result for search by Column" + hit.getSourceAsMap());
        }
    }

    public static void getEmpCount(RestHighLevelClient client, String collectionName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        long count = response.getHits().getTotalHits().value;
        System.out.println("Total no of employee count for " + collectionName + ": " + count);
    }

    public static void delEmpById(RestHighLevelClient client, String collectionName, String employeeId) throws IOException {
        DeleteRequest request = new DeleteRequest(collectionName, employeeId);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println("Delete employed by Id :" + employeeId);
    }

    public static void getDepFacet(RestHighLevelClient client, String collectionName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("dep_count").field("Department.keyword");
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        Terms terms = response.getAggregations().get("dep_count");
        Map<String, Long> results = new HashMap<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            results.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        Gson gson = new Gson();
        System.out.println("Total no of groups in department :" + gson.toJson(results));
    }

    public static void main(String[] args) {
        try (RestHighLevelClient client = createClient()) {
            String vNameCollection = "kavidesh";
            String vPhoneCollection = "7842";


            getDepFacet(client, vNameCollection);
            getDepFacet(client, vPhoneCollection);



        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
