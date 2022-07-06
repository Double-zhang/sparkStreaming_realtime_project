package com.example.publisherrealtime.mapper;

import com.example.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface PublisherMapper {
    Map<String, Object> serachDau(String td);

    List<NameValue> searchStatsByItem(String itemName, String date, String field);

    Map<String, Object> searchDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize);
}
