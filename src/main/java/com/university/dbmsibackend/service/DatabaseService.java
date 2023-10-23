package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.Catalog;
import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.dto.CreateDatabaseRequest;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
@AllArgsConstructor
public class DatabaseService {
    private JsonUtil jsonUtil;

    public void createDatabase(CreateDatabaseRequest request) {
        Database database = new Database(request.name());
        Catalog catalog = jsonUtil.getCatalog();
        catalog.getDatabases().add(database);
        jsonUtil.saveCatalog(catalog);
    }

    public void dropDatabase(String databaseName) {
        Catalog catalog = jsonUtil.getCatalog();
        catalog.setDatabases(
                catalog.getDatabases()
                        .stream()
                        .filter(db -> !Objects.equals(db.getName(), databaseName))
                        .toList()
        );
        jsonUtil.saveCatalog(catalog);
    }

    public List<Database> getDatabases() {
        Catalog catalog = jsonUtil.getCatalog();
        return catalog.getDatabases();
    }
}
