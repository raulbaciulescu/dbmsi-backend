package com.university.dbmsibackend.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.Catalog;
import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.dto.CreateDatabaseRequest;
import com.university.dbmsibackend.exception.EntityAlreadyExistsException;
import com.university.dbmsibackend.service.api.DatabaseService;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.validator.DbsmiValidator;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
@AllArgsConstructor
public class DatabaseServiceImpl implements DatabaseService {
    private JsonUtil jsonUtil;
    private MongoClient mongoClient;

    @Override
    public void createDatabase(CreateDatabaseRequest request) throws EntityAlreadyExistsException {
        Database database = new Database(request.name());
        Catalog catalog = jsonUtil.getCatalog();
        if (!DbsmiValidator.isValidDatabase(catalog.getDatabases(), database))
            throw new EntityAlreadyExistsException("Database already exists!");
        catalog.getDatabases().add(database);
        jsonUtil.saveCatalog(catalog);
    }

    @Override
    public void dropDatabase(String databaseName) {
        Catalog catalog = jsonUtil.getCatalog();
        catalog.setDatabases(
                catalog.getDatabases()
                        .stream()
                        .filter(db -> !Objects.equals(db.getName(), databaseName))
                        .toList()
        );
        jsonUtil.saveCatalog(catalog);
        dropDatabaseFromMongo(databaseName);
    }

    @Override
    public List<Database> getDatabases() {
        Catalog catalog = jsonUtil.getCatalog();
        return catalog.getDatabases();
    }

    private void dropDatabaseFromMongo(String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        database.drop();
    }
}
