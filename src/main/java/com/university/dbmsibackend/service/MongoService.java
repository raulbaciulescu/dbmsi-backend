package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.util.TableMapper;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;

@Service
@AllArgsConstructor
public class MongoService {
    private MongoClient mongoClient;

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
