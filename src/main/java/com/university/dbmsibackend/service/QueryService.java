package com.university.dbmsibackend.service;

import com.university.dbmsibackend.dto.QueryRequest;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class QueryService {

    public void executeQuery(QueryRequest request) {
        System.out.println(request.query());
    }
}
