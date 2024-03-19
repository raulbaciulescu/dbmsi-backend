package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.*;
import com.university.dbmsibackend.dto.CreateForeignKeyRequest;
import com.university.dbmsibackend.dto.CreateIndexRequest;
import com.university.dbmsibackend.service.api.ForeignKeyService;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@AllArgsConstructor
public class ForeignKeyServiceImpl implements ForeignKeyService {
    private JsonUtil jsonUtil;
    private IndexServiceImpl indexService;

    @Override
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
            addIndexFilesForForeignKeyAttributes(request.name(), request.attributes(), request.tableName(), request.databaseName());
        }
    }

    private void addIndexFilesForForeignKeyAttributes(String foreignKeyName, List<Attribute> attributes, String tableName, String databaseName) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(
                foreignKeyName,
                false,
                "BTree",
                tableName,
                databaseName,
                attributes
        );
        indexService.createIndex(createIndexRequest);
    }

    public boolean isAttributeForeignKey(Attribute attribute, String tableName, String databaseName) {
        Catalog catalog = jsonUtil.getCatalog();
        AtomicBoolean result = new AtomicBoolean(false);
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), databaseName))
                .findFirst();

        if (optionalDatabase.isPresent()) {
            Optional<Table> optionalTable = optionalDatabase.get().getTables()
                    .stream()
                    .filter(t -> Objects.equals(t.getName(),tableName))
                    .findFirst();
            optionalTable.ifPresent(table ->
                table.getForeignKeys().forEach(foreignKey -> {
                    if (foreignKey.getAttributes().contains(attribute)) {
                        result.set(true);
                    }
                }));
            jsonUtil.saveCatalog(catalog);
        }
        return result.get();
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
