package com.university.dbmsibackend.dto;

import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.ForeignKey;
import com.university.dbmsibackend.domain.PrimaryKey;

import java.util.List;

public record CreateTableRequest(
        String databaseName,
        String tableName,
        List<Attribute> attributes,
        List<String> primaryKeys,
        List<ForeignKey> foreignKeys
) {
}
