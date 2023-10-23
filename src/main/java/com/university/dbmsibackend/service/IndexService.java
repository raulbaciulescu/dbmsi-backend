package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.Catalog;
import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.domain.Index;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.CreateIndexRequest;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Optional;

@Service
@AllArgsConstructor
public class IndexService {
    private JsonUtil jsonUtil;

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
                .attributes(request.attributes())
                .build();
        table.getIndexes().add(index);
    }
}