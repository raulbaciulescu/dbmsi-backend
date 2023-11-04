package com.university.dbmsibackend.service;

import com.google.gson.Gson;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.dto.SelectAllRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    public List<SelectAllResponse> selectAll(String databaseName, String tableName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        FindIterable<Document> documents = collection.find();
        System.out.println(documents);
        Iterator it = documents.iterator();
        List<SelectAllResponse> response = new ArrayList<>();
        while (it.hasNext()) {
            Document d = (Document) it.next();
            response.add(new SelectAllResponse(d.get("key").toString(), d.get("value").toString()));
        }
        return response;
    }
}
