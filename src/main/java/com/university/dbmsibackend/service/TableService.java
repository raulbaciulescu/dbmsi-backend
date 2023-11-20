package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.university.dbmsibackend.domain.*;
import com.university.dbmsibackend.dto.CreateIndexRequest;
import com.university.dbmsibackend.dto.CreateTableRequest;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.exception.EntityAlreadyExistsException;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.validator.DbsmiValidator;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class TableService {
    private JsonUtil jsonUtil;
    private MongoClient mongoClient;
    private IndexService indexService;

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
            createTableInMongo(request.tableName(), request.databaseName());
            indexService.addIndexFilesForUniqueAttributes(request.attributes(), request.tableName(), request.databaseName());
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
        Document document = new Document("_id", request.key()).append("value", request.value());
        collection.insertOne(document);

        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), request.databaseName()))
                .findFirst();
        if (optionalDatabase.isPresent()) {
            Optional<Table> optionalTable = optionalDatabase.get().getTables().stream().filter(t -> Objects.equals(t.getName(), request.tableName())).findFirst();
            if (optionalTable.isPresent()) {
                List<Index> indexList = optionalTable.get().getIndexes();
                for (Index index : indexList) {
                    List<String> attributesValue = List.of(request.value().split("#"));
                    List<String> indexAttributes = index.getAttributes().stream().map(Attribute::getName).toList();
                    int untilPrimaryKey = optionalTable.get().getPrimaryKeys().size();

                    List<String> attributes = optionalTable.get()
                            .getAttributes()
                            .stream()
                            .map(Attribute::getName)
                            .collect(Collectors.toList());
                    attributes.subList(0, untilPrimaryKey).clear();
                    System.out.println("attributes value: " + attributesValue);
                    System.out.println("attributes: " + attributes);
                    // firstName, lastName, unique, something
                    // da, da, a, da
                    List<String> indexValues = new ArrayList<>();
                    for (int i = 0; i < attributes.size(); i++) {
                        if (indexAttributes.contains(attributes.get(i)))
                            indexValues.add(attributesValue.get(i));
                    }
                    System.out.println("index value: " + indexValues);
                    if (index.getIsUnique()) {
                        indexService.insertInUniqueIndex(request, String.join("#", indexValues), index.getName());
                    }
                    else
                        indexService.insertInNonUniqueIndex(request, String.join("#", indexValues), index.getName());
                }
            }
        }
    }

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

    public void deleteRow(String databaseName, String tableName, List<String> primaryKeys) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (primaryKeys.stream().anyMatch(s -> Objects.equals(s, document.get("_id").toString())))
                collection.deleteMany(document);
        }
    }

    private void createTableInMongo(String tableName, String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        database.createCollection(tableName + ".kv");
    }
}
