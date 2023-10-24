package com.university.dbmsibackend.dto;

import com.university.dbmsibackend.domain.Attribute;

import java.util.List;

public record CreateForeignKeyRequest(
        String name,
        String databaseName,
        String tableName,
        List<Attribute> attributes,
        String referenceTable,
        List<Attribute> referenceAttributes
) {
}
