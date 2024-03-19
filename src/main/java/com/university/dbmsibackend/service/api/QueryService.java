package com.university.dbmsibackend.service.api;

import com.university.dbmsibackend.dto.QueryRequest;

import java.util.List;
import java.util.Map;

public interface QueryService {
    List<Map<String, String>> executeQuery(QueryRequest request);
}
