package com.university.dbmsibackend.service.api;

import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Index;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.CreateIndexRequest;
import com.university.dbmsibackend.dto.InsertRequest;

import java.util.List;

public interface IndexService {
    void createIndex(CreateIndexRequest request);

    void saveAllRowsInIndexFile(Index index, String databaseName, Table table);

    void addIndexFilesForUniqueAttributes(List<Attribute> attributes, String tableName, String databaseName);

    void insertInIndexFiles(Table table, InsertRequest request);
}
