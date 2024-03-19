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
import com.university.dbmsibackend.service.api.IndexService;
import com.university.dbmsibackend.service.api.TableService;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.TableMapper;
import com.university.dbmsibackend.validator.DbsmiValidator;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class TableServiceImpl implements TableService {
    private JsonUtil jsonUtil;
    private MongoClient mongoClient;
    private MongoService mongoService;
    private IndexService indexService;

    @Override
    public List<SelectAllResponse> selectAll(String databaseName, String tableName) {
        return mongoService.selectAll(databaseName, tableName);
    }

    @Override
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

    @Override
    public void dropTable(String databaseName, String tableName) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), databaseName))
                .findFirst();

        if (optionalDatabase.isPresent()) {
            checkIfTableIsLinkedWithOtherTables(optionalDatabase.get(), tableName);
            dropTableFromMongo(tableName, databaseName);
            Optional<Table> optionalTable = optionalDatabase.get().getTables()
                    .stream()
                    .filter(t -> Objects.equals(t.getName(), tableName))
                    .findFirst();
            optionalTable.ifPresent((table) -> dropIndexFiles(table, databaseName));

            optionalDatabase.get().setTables(
                    optionalDatabase.get().getTables()
                            .stream()
                            .filter(t -> !Objects.equals(t.getName(), tableName))
                            .toList()
            );
            jsonUtil.saveCatalog(catalog);
        }
    }

    @Override
    public void deleteRow(String databaseName, String tableName, List<String> primaryKeys) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), databaseName))
                .findFirst();
        if (optionalDatabase.isPresent()) {
            Optional<Table> optionalTable = optionalDatabase.get().getTables().stream().filter(t -> Objects.equals(t.getName(), tableName)).findFirst();
            if (optionalTable.isPresent()) {
                MongoDatabase database = mongoClient.getDatabase(databaseName);
                MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
                FindIterable<Document> documents = collection.find();
                List<Table> linkedForeignKeys = getLinkedTableWithForeignKey(databaseName, tableName);
                for (Document document : documents) {
                    if (primaryKeys.stream().anyMatch(s -> Objects.equals(s, document.get("_id").toString())))
                        if (!existsContainsForeignKeys(document, optionalTable.get(), linkedForeignKeys, databaseName)) {
                            collection.deleteOne(document);
                            deleteFromIndexFiles(optionalTable.get(), databaseName, document);
                        } else
                            throw new ForeignKeyViolationException("A foreign key is used in other table!");
                }
            }
        }
    }

    @Override
    public void insertRow(InsertRequest request) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), request.databaseName()))
                .findFirst();
        MongoDatabase database = mongoClient.getDatabase(request.databaseName());
        if (optionalDatabase.isPresent()) {
            Optional<Table> optionalTable = optionalDatabase.get().getTables().stream().filter(t -> Objects.equals(t.getName(), request.tableName())).findFirst();

            optionalTable.ifPresent(table -> {
                Dictionary<String, String> tableEntry = TableMapper.mapKeyValueToTableRow(request.key(), request.value(), table);
                AtomicBoolean doForeignValuesExist = new AtomicBoolean(true);
                table.getForeignKeys().forEach(foreignKey -> {
                    AtomicBoolean doesForeignValueExist = new AtomicBoolean(false);
                    String foreignTableName = foreignKey.getReferenceTable();
                    List<Attribute> attributes = foreignKey.getAttributes();
                    List<Attribute> foreignAttributes = foreignKey.getReferenceAttributes();
                    Optional<Table> optionalForeignTable = optionalDatabase.get().getTables().stream().filter(t -> Objects.equals(t.getName(), foreignTableName)).findFirst();
                    optionalForeignTable.ifPresent(foreignTable -> {
                        MongoCollection<Document> collection = database.getCollection(foreignTable.getName() + ".kv");
                        for (Document document : collection.find()) {
                            Dictionary<String, String> foreignTableEntry = TableMapper.mapMongoEntryToTableRow(document, foreignTable);
                            for (int i = 0; i < foreignKey.getReferenceAttributes().size(); i++) {
                                var foreignValue = foreignTableEntry.get(foreignAttributes.get(i).getName());
                                var value = tableEntry.get(attributes.get(i).getName());
                                if (Objects.equals(
                                        foreignValue,
                                        value)) {
                                    doesForeignValueExist.set(true);
                                }
                            }
                        }
                    });
                    if (!doesForeignValueExist.get()) {
                        doForeignValuesExist.set(false);
                    }
                });
                if (!doForeignValuesExist.get()) {
                    throw new ForeignKeyViolationException("Foreign keys values don't exist!");
                }
            });

            optionalTable.ifPresent(table -> indexService.insertInIndexFiles(table, request));
        }

        MongoCollection<Document> collection = database.getCollection(request.tableName() + ".kv");
        if (!DbsmiValidator.isValidRow(collection, request)) {
            throw new EntityAlreadyExistsException("A row with that primary key exists!");
        }
        Document document = new Document("_id", request.key()).append("value", request.value());
        collection.insertOne(document);
    }

    private void dropIndexFiles(Table table, String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        table.getIndexes().forEach((index -> {
            MongoCollection<Document> collection = database.getCollection(table.getName() + "_" + index.getName() + ".ind");
            collection.drop();
        }));
    }

    private void dropTableFromMongo(String tableName, String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
        collection.drop();
    }

    private void checkIfTableIsLinkedWithOtherTables(Database database, String tableName) {
        for (Table table : database.getTables()) {
            for (ForeignKey foreignKey : table.getForeignKeys()) {
                if (Objects.equals(foreignKey.getReferenceTable(), tableName))
                    throw new ForeignKeyViolationException("Table is linked to " + table.getName() + " table!");
            }
        }
    }

    private void deleteFromIndexFiles(Table table, String databaseName, Document document) {
        table.getIndexes().forEach(index -> {
            MongoDatabase database = mongoService.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(table.getName() + "_" + index.getName() + ".ind");
            if (index.getIsUnique()) {
                Document query = new Document("primary-key", document.get("_id"));
                collection.deleteOne(query);
            } else {
                FindIterable<Document> documents = collection.find();
                for (Document d : documents) {
                    String s = d.get("primary-key").toString();
                    List<String> sList = List.of(s.split("\\$"));
                    if (sList.contains(document.get("_id").toString())) {
                        String updatedPrimaryKeys = sList.stream()
                                .filter(dString ->
                                        !Objects.equals(dString, document.get("_id").toString()))
                                .collect(Collectors.joining("$"));
                        Document update = new Document("$set", new Document("_id", d.get("_id")).append("primary-key", updatedPrimaryKeys));
                        if (updatedPrimaryKeys.equals(""))
                            collection.deleteOne(d);
                        else
                            collection.updateOne(d, update);
                    }
                }
            }
        });
    }

    private List<Table> getLinkedTableWithForeignKey(String databaseName, String tableName) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), databaseName))
                .findFirst();
        List<Table> linkedForeignKeys = new ArrayList<>();
        optionalDatabase.ifPresent(db ->
                db.getTables().forEach(table ->
                        table.getForeignKeys().forEach(fk -> {
                                    if (Objects.equals(fk.getReferenceTable(), tableName))
                                        linkedForeignKeys.add(table);
                                }
                        ))
        );

        return linkedForeignKeys;
    }

    private boolean existsContainsForeignKeys(Document document, Table table, List<Table> referenceTables, String databaseName) {
        // document are id ul groupului pe care vreau sa l sterg
        Dictionary<String, String> resultRow = TableMapper.mapKeyValueToTableRow(document.getString("_id"), document.getString("value"), table);
        // {id: 1, name: "da", uniqueField: "DA"}

        boolean check = false;
        for (Table referenceTable : referenceTables) {
            for (ForeignKey foreignKey : referenceTable.getForeignKeys()) {
                List<String> attributeNames = foreignKey.getReferenceAttributes().stream().map(Attribute::getName).toList();
                StringBuilder key = new StringBuilder();
                for (String atrName : attributeNames) {
                    key.append(resultRow.get(atrName));
                }
                if (existsForeignKeyInIndexFile(key.toString(), foreignKey.getName(), referenceTable.getName(), databaseName))
                    check = true;
            }
        }

        return check;
    }

    private boolean existsForeignKeyInIndexFile(String checkedKey, String foreignKeyName, String tableName, String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(tableName + "_" + foreignKeyName + ".ind");
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (Objects.equals(checkedKey, document.get("_id").toString()))
                return true;
        }
        return false;
    }

    private void createTableInMongo(String tableName, String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        database.createCollection(tableName + ".kv");
    }
}
