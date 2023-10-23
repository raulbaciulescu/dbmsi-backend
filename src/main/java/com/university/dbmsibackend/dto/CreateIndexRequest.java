package com.university.dbmsibackend.dto;

import com.university.dbmsibackend.domain.Attribute;

import java.util.List;

public record CreateIndexRequest(
        String name,
        Boolean isUnique,
        String type,
        String tableName,
        String databaseName,
        List<Attribute> attributes
) {
}
