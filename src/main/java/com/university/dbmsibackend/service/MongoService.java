package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Index;
import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import com.university.dbmsibackend.util.TableMapper;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class MongoService {
    private MongoClient mongoClient;
    private JsonUtil jsonUtil;

    public List<SelectAllResponse> selectAll(String databaseName, String tableName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        FindIterable<Document> documents = collection.find().limit(1_000_000);
        List<SelectAllResponse> response = new ArrayList<>();
        for (Document document : documents) {
            response.add(new SelectAllResponse(document.get("_id").toString(), document.get("value").toString()));
        }

        return response;
    }

    public List<Map<String, String>> getTableJsonList(String tableName, String databaseName) {
        Table table1 = jsonUtil.getTable(tableName, databaseName);
        List<SelectAllResponse> table1Rows;
        List<Map<String, String>> tableRowsJsons;

        table1Rows = selectAll(databaseName, tableName);
        tableRowsJsons = table1Rows
                .stream()
                .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table1)))
                .collect(Collectors.toList());

        return tableRowsJsons;
    }

    public List<IndexFileValue> getIndexValues(String tableName, String column, String databaseName) {
        List<IndexFileValue> indexFileValues = new ArrayList<>();
        Table table = jsonUtil.getTable(tableName, databaseName);
        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            List<String> attributeNames = index.getAttributes().stream().map(Attribute::getName).toList();
            if (attributeNames.contains(column)) {
                MongoDatabase database = getDatabase(databaseName);
                MongoCollection<Document> collection = database.getCollection(tableName + "_" + index.getName() + ".ind");
                FindIterable<Document> documents = collection.find();
                indexFileValues = Mapper.mapToIndexFileValue(documents);
            }
        }

        return indexFileValues;
    }

    public MongoDatabase getDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }

    public List<SelectAllResponse> getByPrimaryKeys(String databaseName, String tableName, List<String> primaryKeys) {
        List<SelectAllResponse> response = new ArrayList<>();
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        FindIterable<Document> documents = collection.find(Filters.in("_id", primaryKeys));
        for (Document document : documents) {
            response.add(new SelectAllResponse(document.get("_id").toString(), document.get("value").toString()));
        }

        return response;
    }

    public List<Map<String, String>> getByPrimaryKeys(Table table, String databaseName, List<String> primaryKeys) {
        List<Map<String, String>> response = new ArrayList<>();
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(table.getName() + ".kv");
        FindIterable<Document> documents = collection.find(Filters.in("_id", primaryKeys));
        for (Document document : documents) {
            response.add(TableMapper.mapKeyValueToTableRow2(document.get("_id").toString(), document.get("value").toString(), table));
        }

        return response;
    }

    public Map<String, String> getByPrimaryKey(Table table, String primaryKey, String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(table.getName() + ".kv");
        FindIterable<Document> documents = collection.find(Filters.eq("_id", primaryKey));
        for (Document document : documents) {
            return TableMapper.mapKeyValueToTableRow2(document.get("_id").toString(), document.get("value").toString(), table);
        }
        return null;
    }
}
