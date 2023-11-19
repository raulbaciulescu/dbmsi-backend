package com.university.dbmsibackend.validator;

import com.mongodb.client.MongoCollection;
import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.dto.InsertRequest;
import org.bson.Document;

import java.util.List;
import java.util.Objects;

public class DbsmiValidator {
    public static boolean isValidTable(Database database, String tableName) {
        return database.getTables().stream()
                .noneMatch(t -> Objects.equals(t.getName(), tableName));
    }

    public static boolean isValidDatabase(List<Database> databases, Database database) {
        return databases.stream()
                .noneMatch(db -> Objects.equals(db.getName(), database.getName()));
    }

    public static boolean isValidRow(MongoCollection<Document> collection, InsertRequest request) {
        String primaryKey = request.key();
        for (Document document : collection.find()) {
            if (Objects.equals(primaryKey, document.get("key").toString()))
                return false;
        }
        return true;
    }
}
