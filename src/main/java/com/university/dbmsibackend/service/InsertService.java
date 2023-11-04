package com.university.dbmsibackend.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.dto.InsertRequest;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class InsertService {
    private MongoClient mongoClient;

    public void insert(InsertRequest request) {
        MongoDatabase database = mongoClient.getDatabase(request.databaseName());
        MongoCollection<Document> collection = database.getCollection(request.tableName() + ".kv");
        Document document = new Document("key", request.key())
                .append("value", request.value());

        collection.insertOne(document);
    }
}
