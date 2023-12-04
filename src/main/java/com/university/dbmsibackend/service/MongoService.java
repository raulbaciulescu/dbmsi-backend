package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.dto.SelectAllResponse;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
public class MongoService {
    private MongoClient mongoClient;

    public List<SelectAllResponse> selectAll(String databaseName, String tableName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        FindIterable<Document> documents = collection.find();
        List<SelectAllResponse> response = new ArrayList<>();
        for (Document document : documents) {
            response.add(new SelectAllResponse(document.get("_id").toString(), document.get("value").toString()));
        }

        return response;
    }

    public MongoDatabase getDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }
}
