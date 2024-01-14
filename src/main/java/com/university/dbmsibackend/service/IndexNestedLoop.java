package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.*;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import com.university.dbmsibackend.util.TableMapper;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;


@Service
public class IndexNestedLoop {
    @Autowired
    private JsonUtil jsonUtil;
    @Autowired
    private MongoService mongoService;

    public List<Map<String, String>> doJoin(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        boolean hasIndex1 = hasIndex(tableName1, column1, databaseName);
        boolean hasIndex2 = hasIndex(tableName2, column2, databaseName);
        if (!hasIndex2 && hasIndex1) {
            String temp = tableName1;
            tableName1 = tableName2;
            tableName2 = temp;

            temp = column1;
            column1 = column2;
            column2 = temp;
        } else if (!hasIndex1) { // nestedLoop
            return simpleNestedLoop(tableName1, tableName2, column1, column2, databaseName, predicate);
        }

        List<Map<String, String>> table1RowsJsons = getTableJsonList(tableName1, databaseName);
        List<IndexFileValue> table2IndexValues = getIndexValues(tableName2, column2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Table table2 = jsonUtil.getTable(tableName2, databaseName);
        for (Map<String, String> map1 : table1RowsJsons) {
            for (IndexFileValue indexFileValue : table2IndexValues) {
                if (compare(predicate, indexFileValue.value(), map1.get(column1))) {
                    result.addAll(merge(map1, indexFileValue.primaryKeys(), tableName1, table2, databaseName));
                }
            }
        }

        return result;
    }

    private List<Map<String, String>> merge(Map<String, String> map1, List<String> primaryKeys, String tableName1, Table table2, String databaseName) {
        List<Map<String, String>> result = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            Map<String, String> commmonMap = new HashMap<>();
            Map<String, String> map2 = getMapByPrimaryKey(table2, primaryKey, databaseName);
            for (String key : map1.keySet()) {
                commmonMap.put(tableName1 + "." + key, map1.get(key));
            }
            for (String key : map2.keySet()) {
                commmonMap.put(table2.getName() + "." + key, map2.get(key));
            }
            result.add(commmonMap);
        }

        return result;
    }

    private Map<String, String> getMapByPrimaryKey(Table table, String primaryKey, String databaseName) {
        return mongoService.getByPrimaryKey(table, primaryKey, databaseName);
    }

    private List<Map<String, String>> simpleNestedLoop(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        List<Map<String, String>> table1RowsJsons = getTableJsonList(tableName1, databaseName);
        List<Map<String, String>> table2RowsJsons = getTableJsonList(tableName2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Table table2 = jsonUtil.getTable(tableName2, databaseName);
        for (Map<String, String> map1 : table1RowsJsons) {
            for (Map<String, String> map2 : table1RowsJsons) {
                if (compare(predicate, map2.get(column2), map1.get(column1))) {
                    result.add(mergeMaps(map1, map2, tableName1, tableName2));
                }
            }
        }

        return result;
    }

    private Map<String, String> mergeMaps(Map<String, String> map1, Map<String, String> map2, String tableName1, String tableName2) {
        Map<String, String> commmonMap = new HashMap<>();
        for (String key : map1.keySet()) {
            commmonMap.put(tableName1 + "." + key, map1.get(key));
        }
        for (String key : map2.keySet()) {
            commmonMap.put(tableName2 + "." + key, map2.get(key));
        }

        return commmonMap;
    }

    private boolean compare(Operation predicate, String value, String s) {
        return Objects.equals(value, s);
    }

    private boolean hasIndex(String tableName, String column, String databaseName) {
        Table table = jsonUtil.getTable(tableName, databaseName);
        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            List<String> attributeNames = index.getAttributes().stream().map(Attribute::getName).toList();
            if (attributeNames.contains(column)) {
                    return true;
            }
        }

        return false;
    }

    private List<IndexFileValue> getIndexValues(String tableName, String column, String databaseName) {
        List<IndexFileValue> indexFileValues = new ArrayList<>();
        Table table = jsonUtil.getTable(tableName, databaseName);
        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            List<String> attributeNames = index.getAttributes().stream().map(Attribute::getName).toList();
            if (attributeNames.contains(column)) {
                MongoDatabase database = mongoService.getDatabase(databaseName);
                MongoCollection<Document> collection = database.getCollection(tableName + "_" + index.getName() + ".ind");
                FindIterable<Document> documents = collection.find();
                indexFileValues = Mapper.mapToIndexFileValue(documents);
            }
        }

        return indexFileValues;
    }

    public List<Map<String, String>> getTableJsonList(String tableName, String databaseName) {
        Table table1 = jsonUtil.getTable(tableName, databaseName);
        List<SelectAllResponse> table1Rows;
        List<Map<String, String>> tableRowsJsons;

        table1Rows = mongoService.selectAll(databaseName, tableName);
        tableRowsJsons = table1Rows
                .stream()
                .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table1)))
                .toList();

        return tableRowsJsons;
    }
}
