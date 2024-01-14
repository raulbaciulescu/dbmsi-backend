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
public class SortMergeJoin {
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
            return sortMergeJoinWithoutIndex(tableName1, tableName2, column1, column2, databaseName, predicate);
        }

        List<Map<String, String>> table1RowsJsons = getTableJsonList(tableName1, databaseName);
        List<IndexFileValue> table2IndexValues = getIndexValues(tableName2, column2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Table table2 = jsonUtil.getTable(tableName2, databaseName);


        String finalColumn1 = column1;
        Comparator<Map<String, String>> mapComparator = new Comparator<Map<String, String>>() {
            public int compare(Map<String, String> m1, Map<String, String> m2) {
                Integer i1 = Integer.parseInt(m1.get(finalColumn1));
                Integer i2 = Integer.parseInt(m2.get(finalColumn1));
                return i1.compareTo(i2);
            }
        };
        String finalColumn = column1;
       // System.out.println(table1RowsJsons);
//        var x = table1RowsJsons.stream()
//                .sorted(mapComparator)
//                .toList();
        table1RowsJsons.sort(mapComparator);
//        System.out.println(" DA " + table1RowsJsons);
//        System.out.println("-------------------------");
//        System.out.println(table2IndexValues);
        table2IndexValues = table2IndexValues.stream()
                .sorted(Comparator.comparing(indexFileValue -> Integer.parseInt(indexFileValue.value())))
                .toList();
      //  System.out.println(table2IndexValues);
        int mark = -1, r = 0, s = 0;
        do {
            if (mark == -1) {
                while (r < table1RowsJsons.size() && compare(predicate, table1RowsJsons.get(r).get(column1), table2IndexValues.get(s).value()) < 0)
                    r++;
                while (s < table2IndexValues.size() && compare(predicate, table1RowsJsons.get(r).get(column1), table2IndexValues.get(s).value()) > 0)
                    s++;
                mark = s;
            }
            if (compare(predicate, table1RowsJsons.get(r).get(column1), table2IndexValues.get(s).value()) == 0) {
                result.addAll(merge(table1RowsJsons.get(r), table2IndexValues.get(s).primaryKeys(), tableName1, table2, databaseName));
                s++;
            } else {
                s = mark;
                r++;
                mark = -1;
            }
        } while (r < table1RowsJsons.size() && s < table2IndexValues.size());

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

    private int compare(Operation predicate, String value, String s) {
        return Integer.compare(Integer.parseInt(value), Integer.parseInt(s));
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
                .collect(Collectors.toList());

        return tableRowsJsons;
    }

    private List<Map<String, String>> sortMergeJoinWithoutIndex(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        return null;
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
}
