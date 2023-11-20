package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.*;
import com.university.dbmsibackend.dto.CreateTableRequest;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.exception.EntityAlreadyExistsException;
import com.university.dbmsibackend.exception.ForeignKeyViolationException;
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
            checkIfTableIsLinkedWithOtherTables(optionalDatabase.get(), tableName);
            optionalDatabase.get().setTables(
                    optionalDatabase.get().getTables()
                            .stream()
                            .filter(t -> !Objects.equals(t.getName(), tableName))
                            .toList()
            );
            jsonUtil.saveCatalog(catalog);
            dropTableFromMongo(tableName, databaseName);
        }
    }

    private void dropTableFromMongo(String tableName, String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        collection.drop();
    }

    private void checkIfTableIsLinkedWithOtherTables(Database database, String tableName) {
        for (Table table: database.getTables()) {
            for (ForeignKey foreignKey : table.getForeignKeys()) {
                if (Objects.equals(foreignKey.getReferenceTable(), tableName))
                    throw new ForeignKeyViolationException("Table is linked to " + table.getName() + " table!");
            }
        }
    }

    public void insertRow(InsertRequest request) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), request.databaseName()))
                .findFirst();
        if (optionalDatabase.isPresent()) {
            Optional<Table> optionalTable = optionalDatabase.get().getTables().stream().filter(t -> Objects.equals(t.getName(), request.tableName())).findFirst();
            optionalTable.ifPresent(table -> indexService.insertInIndexFiles(table, request));
        }

        MongoDatabase database = mongoClient.getDatabase(request.databaseName());
        MongoCollection<Document> collection = database.getCollection(request.tableName() + ".kv");
        if (!DbsmiValidator.isValidRow(collection, request)) {
            throw new EntityAlreadyExistsException("A row with that primary key exists!");
        }
        Document document = new Document("_id", request.key()).append("value", request.value());
        collection.insertOne(document);
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
