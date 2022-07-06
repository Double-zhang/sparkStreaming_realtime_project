package com.example.publisherrealtime.service.impl;

import com.example.publisherrealtime.bean.NameValue;
import com.example.publisherrealtime.mapper.PublisherMapper;
import com.example.publisherrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    PublisherMapper publisherMapper;

    /*
    日活分析业务处理
     */
    @Override
    public Map<String, Object> doDauRealtime(String td) {
        Map<String, Object> dauResults =  publisherMapper.serachDau(td);
        //业务处理
        return dauResults;
    }

    /*
    交易分析业务处理
     */
    @Override
    public List<NameValue> daStatsByItem(String itemName, String date, String t) {

        //业务处理
        List<NameValue> searchResults = publisherMapper.searchStatsByItem(itemName,date,typeToField(t));
        return transformResults(searchResults,t);
    }

    //交易分析明细查询
    @Override
    public Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {
        //业务处理
        //计算分页开始位置
        int from = (pageNo - 1)*pageSize;
        Map<String, Object> searchResults = publisherMapper.searchDetailByItem(date,itemName,from,pageSize);
        return searchResults;
    }

    public List<NameValue> transformResults(List<NameValue> searchResults, String t) {
        if ("gender".equals(t)) {
            if (searchResults.size() > 0) {
                for (NameValue nameValue: searchResults) {
                    String name = nameValue.getName();
                    if (name.equals("F")) nameValue.setName("女");
                    else if (name.equals("M")) nameValue.setName("男");
                }
            }
            return searchResults;
        }else if ("age".equals(t)) {
            double totalAmount20 = 0;
            double totalAmount20to29 = 0;
            double totalAmount30 = 0;
            if (searchResults.size() > 0) {
                for (NameValue nameValue : searchResults) {
                    Integer age = Integer.parseInt(nameValue.getName());
                    double value = Double.parseDouble(nameValue.getValue().toString());
                    if (age < 20) {
                        totalAmount20 += value;
                    }else if (age <= 29) {
                        totalAmount20to29 += value;
                    }else totalAmount30 += value;
                }
                searchResults.clear();
                searchResults.add(new NameValue("20岁以下",totalAmount20));
                searchResults.add(new NameValue("20到29岁",totalAmount20to29));
                searchResults.add(new NameValue("30岁以上",totalAmount30));
            }
            return searchResults;
        }else return null;
    }

    public String typeToField(String t) {
        if ("age".equals(t)) return "user_age";
        else if ("gender".equals(t)) return "user_gender";
        else return null;
    }
}
