package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.university.dbmsibackend.dto.DeleteRowRequest;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.mongodb.client.model.Filters.eq;

@Service
@AllArgsConstructor
public class InsertService {
    private MongoClient mongoClient;

    public void insertRow(InsertRequest request) {
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
        Iterator it = documents.iterator();
        List<SelectAllResponse> response = new ArrayList<>();
        while (it.hasNext()) {
            Document d = (Document) it.next();
            response.add(new SelectAllResponse(d.get("key").toString(), d.get("value").toString()));
        }
        return response;
    }

    public void deleteRow(String databaseName, String tableName, List<String> primaryKeys) {
        System.out.println(databaseName);
        System.out.println(tableName);
        System.out.println(primaryKeys);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (primaryKeys.stream().anyMatch(s -> Objects.equals(s, document.get("key").toString())))
                collection.deleteMany(document);
        }
    }
}
