package com.university.dbmsibackend.util;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.university.dbmsibackend.domain.Catalog;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


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
}
