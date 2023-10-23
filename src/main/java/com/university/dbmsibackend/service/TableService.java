package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.Catalog;
import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.CreateTableRequest;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class TableService {
    private JsonUtil jsonUtil;

    public void createTable(CreateTableRequest request) {
        Catalog catalog = jsonUtil.getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), request.databaseName()))
                .findFirst();

        if (optionalDatabase.isPresent()) {
            Table table = Table
                    .builder()
                    .name(request.tableName())
                    .attributes(request.attributes())
                    .primaryKeys(request.primaryKeys())
                    .foreignKeys(request.foreignKeys())
                    .build();
            optionalDatabase.get().getTables().add(table);
            jsonUtil.saveCatalog(catalog);
        }
    }

    public void dropTable() {

    }
}
