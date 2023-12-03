package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.*;
import com.university.dbmsibackend.dto.CreateIndexRequest;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.exception.EntityAlreadyExistsException;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.TableMapper;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class IndexService {
    private JsonUtil jsonUtil;
    private MongoClient mongoClient;
    //private TableService tableService;

    public void createIndex(CreateIndexRequest request) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), request.databaseName()))
                .findFirst();
        optionalDatabase.ifPresent(database -> {
            Optional<Table> optionalTable = database.getTables()
                    .stream()
                    .filter(t -> Objects.equals(t.getName(), request.tableName()))
                    .findFirst();
            optionalTable.ifPresent(table -> {
                Index index = setIndex(table, request);
                saveAllRowsInIndexFile(index, request.databaseName(), table);
            });
            jsonUtil.saveCatalog(catalog);
        });
    }

    private void saveAllRowsInIndexFile(Index index, String databaseName, Table table) {
        List<SelectAllResponse> rows = selectAll(databaseName, table.getName());
        rows.forEach(row -> {
            List<String> indexValues = getValuesForIndexAttributes2(index, table, row.value());
            insertInNonUniqueIndex2(row.key(), String.join("#", indexValues), index.getName(), databaseName, table.getName());
        });
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

    private List<String> getValuesForIndexAttributes2(Index index, Table table, String value) {
        List<String> attributesValue = List.of(value.split("#"));
        List<String> indexAttributes = index.getAttributes().stream().map(Attribute::getName).toList();
        int untilPrimaryKey = table.getPrimaryKeys().size();

        List<String> attributes = table
                .getAttributes()
                .stream()
                .map(Attribute::getName)
                .collect(Collectors.toList());
        attributes.subList(0, untilPrimaryKey).clear();
        List<String> indexValues = new ArrayList<>();
        for (int i = 0; i < attributes.size(); i++) {
            if (indexAttributes.contains(attributes.get(i)))
                indexValues.add(attributesValue.get(i));
        }

        return indexValues;
    }

    public Index setIndex(Table table, CreateIndexRequest request) {
        Index index = Index
                .builder()
                .name(request.name())
                .type(request.type())
                .isUnique(request.isUnique())
                .attributes(request.attributes())
                .build();
        table.getIndexes().add(index);
        return index;
    }

    public void insertInIndexFiles(Table table, InsertRequest request) {
        List<Index> indexList = table.getIndexes();
        for (Index index : indexList) {
            List<String> indexValues = getValuesForIndexAttributes(index, table, request);
            if (index.getIsUnique())
                checkForErrors(request, String.join("#", indexValues), index.getName());
        }

        for (Index index : indexList) {
            List<String> indexValues = getValuesForIndexAttributes(index, table, request);
            if (index.getIsUnique()) {
                insertInUniqueIndex(request, String.join("#", indexValues), index.getName());
            } else
                insertInNonUniqueIndex(request, String.join("#", indexValues), index.getName());
        }
    }

    private void checkForErrors(InsertRequest request, String uniqueKey, String indexName) {
        MongoDatabase database = mongoClient.getDatabase(request.databaseName());
        MongoCollection<Document> collection = database.getCollection(request.tableName() + "_" + indexName + ".ind");
        Document query = new Document("_id", uniqueKey);
        Document result = collection.find(query).first();
        if (result != null) {
            throw new EntityAlreadyExistsException("Unique key already exists!");
        }
    }

    /**
     * ex: pt tabelul student(id, firstName, lastName) avem index pe firstName, lastName
     * si urmatorul rand de adaugat (1, raul, baciulescu) se va returna [raul, baciulescu]
     *
     * @return returneaza valorile pentru coloanele din tabel care au index
     */
    public List<String> getValuesForIndexAttributes(Index index, Table table, InsertRequest request) {
        List<String> attributesValue = List.of(request.value().split("#"));
        List<String> indexAttributes = index.getAttributes().stream().map(Attribute::getName).toList();
        int untilPrimaryKey = table.getPrimaryKeys().size();

        List<String> attributes = table
                .getAttributes()
                .stream()
                .map(Attribute::getName)
                .collect(Collectors.toList());
        attributes.subList(0, untilPrimaryKey).clear();
        List<String> indexValues = new ArrayList<>();
        for (int i = 0; i < attributes.size(); i++) {
            if (indexAttributes.contains(attributes.get(i)))
                indexValues.add(attributesValue.get(i));
        }

        return indexValues;
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

    public void insertInNonUniqueIndex2(String primaryKey, String nonUniqueKey, String indexName, String databaseName, String tableName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + "_" + indexName + ".ind");
        Document query = new Document("_id", nonUniqueKey);
        Document result = collection.find(query).first();
        if (result != null) {
            String newPrimaryKeys = result.get("primary-key").toString() + "$" + primaryKey;
            Document update = new Document("$set", new Document("_id", nonUniqueKey).append("primary-key", newPrimaryKeys));
            collection.updateOne(query, update);
        } else {
            Document document = new Document("_id", nonUniqueKey).append("primary-key", primaryKey);
            collection.insertOne(document);
        }
    }

    public void insertInUniqueIndex(InsertRequest request, String uniqueKey, String indexName) {
        MongoDatabase database = mongoClient.getDatabase(request.databaseName());
        MongoCollection<Document> collection = database.getCollection(request.tableName() + "_" + indexName + ".ind");
        Document query = new Document("_id", uniqueKey);
        Document result = collection.find(query).first();
        if (result != null) {
            throw new EntityAlreadyExistsException("Unique key already exists!");
        } else {
            Document document = new Document("_id", uniqueKey).append("primary-key", request.key());
            collection.insertOne(document);
        }
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