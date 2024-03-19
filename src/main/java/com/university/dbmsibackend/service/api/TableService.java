package com.university.dbmsibackend.service.api;

import com.university.dbmsibackend.dto.CreateTableRequest;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;

import java.util.List;

public interface TableService {
    List<SelectAllResponse> selectAll(String databaseName, String tableName);

    void createTable(CreateTableRequest request);

    void dropTable(String databaseName, String tableName);

    void insertRow(InsertRequest request);

    void deleteRow(String databaseName, String tableName, List<String> primaryKeys);
}
