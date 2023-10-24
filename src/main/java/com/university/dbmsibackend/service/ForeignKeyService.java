package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.*;
import com.university.dbmsibackend.dto.CreateForeignKeyRequest;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Optional;

@Service
@AllArgsConstructor
public class ForeignKeyService {
    private JsonUtil jsonUtil;

    public void createForeignKey(CreateForeignKeyRequest request) {
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
            optionalTable.ifPresent(table -> setForeignKey(table, request));
            jsonUtil.saveCatalog(catalog);
        }
    }

    private void setForeignKey(Table table, CreateForeignKeyRequest request) {
        ForeignKey foreignKey = ForeignKey
                .builder()
                .name(request.name())
                .attributes(request.attributes())
                .referenceTable(request.referenceTable())
                .referenceAttributes(request.referenceAttributes())
                .build();
        table.getForeignKeys().add(foreignKey);
    }
}
