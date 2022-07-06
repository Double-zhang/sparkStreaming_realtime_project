package com.example.publisherrealtime.mapper.impl;

import com.example.publisherrealtime.bean.NameValue;
import com.example.publisherrealtime.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    //Es的客户端对象
    @Autowired
    RestHighLevelClient esClient;

    private String indexNamePrefix = "gmall_dau_info_1018_";
    private String orderIndexNamePrefix = "gmall_order_wide_1018_";

    public Long searchDauTotal(String td) {
        String indexName = indexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);  //不要明细
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long dauTotal = searchResponse.getHits().getTotalHits().value;
            return dauTotal;
        }catch (ElasticsearchStatusException ese){
            if (ese.status() == RestStatus.NOT_FOUND)
                log.warn(indexName + "索引不存在。。。。。。。。。。。。");
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败。。。。。。。。。。。");
        }
        return 0L;
    }

    //查询分时明细

    public Map<String,Long> searchDauHr(String td) {
        Map<String, Long> dauHr = new HashMap<>();
        String indexName = indexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);  //不要明细
        //聚合
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupbyhr").field("hr").size(24);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupbyhr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String hr = bucket.getKeyAsString();
                long hrTotal = bucket.getDocCount();
                dauHr.put(hr,hrTotal);
            }
            return dauHr;
        }catch (ElasticsearchStatusException ese){
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在。。。。");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败。。。。");
        }
        return dauHr;
    }

    @Override

    public Map<String, Object> serachDau(String td) {
        Map<String,Object> dauResults = new HashMap<>();
        //日活总数
        Long dauTotal = searchDauTotal(td);
        dauResults.put("dauTotal", dauTotal);

        //今日分时明细
        Map<String, Long> dauId = searchDauHr(td);
        dauResults.put("dauId",dauId);

        //昨日分时明细
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);  //当前天减一天
        String yd = ydLd.toString();
        Map<String, Long> dauYd = searchDauHr(yd);
        dauResults.put("dauYd",dauYd);

        return dauResults;
    }

    /**
     * 交易分析 -- 按照类别（年龄、性别）统计
     * @param itemName
     * @param date
     * @param field
     * @return
     */
    @Override

    public List<NameValue> searchStatsByItem(String itemName, String date, String field) {
        ArrayList<NameValue> result = new ArrayList<>();
        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);  //不需要明细
        //query     ES中query查找的代码写法。。。根据DSL语句写的
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);
        //group
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby" + field).field(field).size(100);
        //sum
        //根据filed字段做sum操作
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("totalamount").field("split_total_amount");
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupby" + field);
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String key = bucket.getKeyAsString();
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedSum parsedSum = bucketAggregations.get("totalamount");
                double totalamount = parsedSum.getValue();
                result.add(new NameValue(key,totalamount));
            }
            return result;
        } catch (ElasticsearchStatusException ese){
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "索引不存在。。。。。。");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败。。。。。。。。");
        }
        return result;
    }


    //交易业务明细查询
    @Override
    public Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize) {
        HashMap<String, Object> results = new HashMap<>();
        String indexName = orderIndexNamePrefix+date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"create_time"},null);  //数组里放要查哪些特定字段
        //query
        /*
        对应着"query": { "query": { "match":  { "sku_name": "query": 小米手机 " ,  "operator": "and" } } }
         */
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);//构建一个分词查询
        searchSourceBuilder.query(matchQueryBuilder);
        //from
        searchSourceBuilder.from(from);
        //size
        searchSourceBuilder.size(pageSize);

        //高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("sku_name");
        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);  //响应结果
            long total = searchResponse.getHits().getTotalHits().value;
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            ArrayList<Map<String, Object>> maps = new ArrayList<>();
            for (SearchHit searchHit : searchHits) {

                //提取source
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                //提取高亮
                Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("sku_name");
                Text[] fragments = highlightField.getFragments();
                String s = fragments[0].toString();
                //使用高亮结果覆盖原结果
                sourceAsMap.put("sku_name",s);
                maps.add(sourceAsMap);
            }
            //最终结果
            results.put("total",total);
            results.put("detail",maps);
        } catch (ElasticsearchStatusException ese){
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "索引不存在。。。。。。");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败。。。。。。。。");
        }
        return null;
    }
}
