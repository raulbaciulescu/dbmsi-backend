package com.university.dbmsibackend.dto;

public record DeleteRowRequest(String primaryKey, String tableName, String databaseName) {
}
