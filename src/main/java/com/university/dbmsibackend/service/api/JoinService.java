package com.university.dbmsibackend.service.api;

import com.university.dbmsibackend.domain.Operation;

import java.util.List;
import java.util.Map;

public interface JoinService {
    List<Map<String, String>> doJoin(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate);

    List<Map<String, String>> secondJoin(List<Map<String, String>> rows, String tableName1, String tableName2, String column1, String column2, String databaseName, Operation operation);
}
