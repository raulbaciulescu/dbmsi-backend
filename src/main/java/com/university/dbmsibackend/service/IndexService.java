package com.university.dbmsibackend.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.*;
import com.university.dbmsibackend.dto.CreateIndexRequest;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
@AllArgsConstructor
public class IndexService {
    private JsonUtil jsonUtil;
    private MongoClient mongoClient;

    public void createIndex(CreateIndexRequest request) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), request.databaseName()))
                .findFirst();

        if (optionalDatabase.isPresent()) {
            Optional<Table> optionalTable = optionalDatabase.get().getTables()
                    .stream()
                    .filter(t -> Objects.equals(t.getName(), request.tableName()))
                    .findFirst();
            optionalTable.ifPresent(table -> setIndex(table, request));
            jsonUtil.saveCatalog(catalog);
        }
    }

    public void setIndex(Table table, CreateIndexRequest request) {
        Index index = Index
                .builder()
                .name(request.name())
                .type(request.type())
                .isUnique(request.isUnique())
                .attributes(request.attributes())
                .build();
        table.getIndexes().add(index);
    }

    public void insertInNonUniqueIndex(InsertRequest request, String nonUniqueKey, String indexName) {
        MongoDatabase database = mongoClient.getDatabase(request.databaseName());
        MongoCollection<Document> collection = database.getCollection(request.tableName() + "_" + indexName + ".ind");
        Document query = new Document("_id", nonUniqueKey);
        Document result = collection.find(query).first();
        if (result != null) {
            String newPrimaryKeys = result.get("primary-key").toString() + "$" + request.key();
            Document update = new Document("$set", new Document("_id", nonUniqueKey).append("primary-key", newPrimaryKeys));
            collection.updateOne(query, update);
        } else {
            Document document = new Document("_id", nonUniqueKey).append("primary-key", request.key());
            collection.insertOne(document);
        }
    }

    public void insertInUniqueIndex(InsertRequest request, String uniqueKey, String indexName) {
        MongoDatabase database = mongoClient.getDatabase(request.databaseName());
        MongoCollection<Document> collection = database.getCollection(request.tableName() + "_" + indexName + ".ind");
        Document document = new Document("_id", uniqueKey).append("primary-key", request.key());
        collection.insertOne(document);
    }

    public void addIndexFilesForUniqueAttributes(List<Attribute> attributes, String tableName, String databaseName) {
        attributes.forEach(attribute -> {
            if (attribute.getIsUnique()) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(
                        attribute.getName(),
                        true,
                        "BTree",
                        tableName,
                        databaseName,
                        List.of(attribute)
                );
                createIndex(createIndexRequest);
            }
        });
    }
}