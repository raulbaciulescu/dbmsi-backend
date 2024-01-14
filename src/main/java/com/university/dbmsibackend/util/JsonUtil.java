package com.university.dbmsibackend.util;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.university.dbmsibackend.domain.*;
import com.university.dbmsibackend.exception.EntityNotFoundException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class JsonUtil {
    public void saveCatalog(Catalog catalog) {
        try {
            Gson gson = new Gson();
            FileWriter file = new FileWriter(Constants.CATALOG_PATH);
            String catalogString = gson.toJson(catalog);
            file.write(catalogString);
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Catalog getCatalog() {
        try {
            Gson gson = new Gson();
            JsonReader reader = new JsonReader(new FileReader(Constants.CATALOG_PATH));
            Catalog catalog = gson.fromJson(reader, Catalog.class);
            if (catalog == null)
                catalog = new Catalog();
            return catalog;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Table getTable(String tableName, String databaseName) {
        Catalog catalog = getCatalog();
        Optional<Database> optionalDatabase = catalog.getDatabases()
                .stream()
                .filter(db -> Objects.equals(db.getName(), databaseName))
                .findFirst();
        if (optionalDatabase.isPresent()) {
            Optional<Table> optionalTable = optionalDatabase.get().
                    getTables().stream()
                    .filter(t -> Objects.equals(t.getName(), tableName)).findFirst();
            if (optionalTable.isPresent())
                return optionalTable.get();
            else throw new EntityNotFoundException("Table doesn't exist!");
        } else throw new EntityNotFoundException("Database doesn't exist!");
    }

    public boolean hasIndex(String tableName, String column, String databaseName) {
        Table table = getTable(tableName, databaseName);
        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            List<String> attributeNames = index.getAttributes().stream().map(Attribute::getName).toList();
            if (attributeNames.contains(column)) {
                return true;
            }
        }

        return false;
    }
}
