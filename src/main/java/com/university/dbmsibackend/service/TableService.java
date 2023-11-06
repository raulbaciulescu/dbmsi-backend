package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.Catalog;
import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.CreateTableRequest;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.exception.EntityAlreadyExistsException;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.validator.DbsmiValidator;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
@AllArgsConstructor
public class TableService {
    private JsonUtil jsonUtil;
    private MongoClient mongoClient;

    public void createTable(CreateTableRequest request) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), request.databaseName()))
                .findFirst();

        if (optionalDatabase.isPresent()) {
            if (!DbsmiValidator.isValidTable(optionalDatabase.get(), request.tableName()))
                throw new EntityAlreadyExistsException("Table already exists!");

            Table table = Table
                    .builder()
                    .name(request.tableName())
                    .attributes(request.attributes())
                    .primaryKeys(request.primaryKeys())
                    .foreignKeys(new ArrayList<>())
                    .indexes(new ArrayList<>())
                    .build();
            optionalDatabase.get().getTables().add(table);
            jsonUtil.saveCatalog(catalog);
        }
    }

    public void dropTable(String databaseName, String tableName) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), databaseName))
                .findFirst();

        if (optionalDatabase.isPresent()) {
            optionalDatabase.get().setTables(
                    optionalDatabase.get().getTables()
                            .stream()
                            .filter(t -> !Objects.equals(t.getName(), tableName))
                            .toList()
            );
            jsonUtil.saveCatalog(catalog);
        }
    }

    public void insertRow(InsertRequest request) {
        MongoDatabase database = mongoClient.getDatabase(request.databaseName());
        MongoCollection<Document> collection = database.getCollection(request.tableName() + ".kv");
        if (!DbsmiValidator.isValidRow(collection, request)) {
            throw new EntityAlreadyExistsException("A row with that primary key exists!");
        }
        Document document = new Document("key", request.key())
                .append("value", request.value());

        collection.insertOne(document);
    }

    public List<SelectAllResponse> selectAll(String databaseName, String tableName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        FindIterable<Document> documents = collection.find();
        List<SelectAllResponse> response = new ArrayList<>();
        for (Document document : documents) {
            response.add(new SelectAllResponse(document.get("key").toString(), document.get("value").toString()));
        }

        return response;
    }

    public void deleteRow(String databaseName, String tableName, List<String> primaryKeys) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (primaryKeys.stream().anyMatch(s -> Objects.equals(s, document.get("key").toString())))
                collection.deleteMany(document);
        }
    }
}
