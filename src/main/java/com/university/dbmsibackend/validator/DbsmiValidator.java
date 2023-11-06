package com.university.dbmsibackend.validator;

import com.university.dbmsibackend.domain.Database;

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
}
