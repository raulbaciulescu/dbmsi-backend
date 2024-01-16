package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Operation;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.service.api.JoinService;
import com.university.dbmsibackend.util.JoinUtil;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;


@Service
@AllArgsConstructor
public class IndexNestedLoopJoinService implements JoinService {
    private JsonUtil jsonUtil;
    private MongoService mongoService;
    private JoinUtil joinUtil;

    @Override
    public List<Map<String, String>> doJoin(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        System.out.println("IndexNestedLoopJoinService");
        boolean hasIndex1 = jsonUtil.hasIndex(tableName1, column1, databaseName);
        boolean hasIndex2 = jsonUtil.hasIndex(tableName2, column2, databaseName);
        if (!hasIndex2 && hasIndex1) {
            String temp = tableName1;
            tableName1 = tableName2;
            tableName2 = temp;

            temp = column1;
            column1 = column2;
            column2 = temp;
        } else if (!hasIndex1 && !hasIndex2) { // nestedLoop
            return simpleNestedLoop(tableName1, tableName2, column1, column2, databaseName, predicate);
        }
        System.out.println("index nested with index");
        List<Map<String, String>> table1RowsJsons = mongoService.getTableJsonList(tableName1, databaseName);
        List<IndexFileValue> table2IndexValues = mongoService.getIndexValues(tableName2, column2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Table table2 = jsonUtil.getTable(tableName2, databaseName);
        for (Map<String, String> map1 : table1RowsJsons) {
            for (IndexFileValue indexFileValue : table2IndexValues) {
                if (compare(predicate, indexFileValue.value(), map1.get(column1))) {
                    result.addAll(joinUtil.mergeMapWithPrimaryKeys(map1, indexFileValue.primaryKeys(), tableName1, table2, databaseName));
                }
            }
        }

        System.out.println(result);

        return result;
    }

    @Override
    public List<Map<String, String>> secondJoin(List<Map<String, String>> rows, String tableName1, String tableName2,
                                                String column1, String column2, String databaseName, Operation operation) {
        System.out.println("second join");
        boolean hasIndex2 = jsonUtil.hasIndex(tableName2, column2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Table table2 = jsonUtil.getTable(tableName2, databaseName);

        if (hasIndex2) {
            System.out.println("second join has index");
            List<IndexFileValue> table2IndexValues = mongoService.getIndexValues(tableName2, column2, databaseName);
            for (Map<String, String> map1 : rows) {
                for (IndexFileValue indexFileValue : table2IndexValues) {
                    if (compare(operation, indexFileValue.value(), map1.get(tableName1 + "." + column1))) {
                        result.addAll(joinUtil.mergeMapWithPrimaryKeys(map1, indexFileValue.primaryKeys(), tableName1, table2, databaseName));
                    }
                }
            }
        } else {
            System.out.println("second join has not index");
            List<Map<String, String>> table2RowsJsons = mongoService.getTableJsonList(tableName2, databaseName);
            for (Map<String, String> map1 : rows) {
                for (Map<String, String> map2 : table2RowsJsons) {
                    if (compare(operation, map2.get(column1), map1.get(tableName2 + "." + column2))) {
                        result.add(joinUtil.mergeMaps(map1, map2, tableName1, tableName2));
                    }
                }
            }
        }
        System.out.println("second join result: " + result);

        return result;
    }

    private List<Map<String, String>> simpleNestedLoop(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        System.out.println("simple nested loop");
        List<Map<String, String>> table1RowsJsons = mongoService.getTableJsonList(tableName1, databaseName);
        List<Map<String, String>> table2RowsJsons = mongoService.getTableJsonList(tableName2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();

        for (Map<String, String> map1 : table1RowsJsons) {
            for (Map<String, String> map2 : table2RowsJsons) {
                if (compare(predicate, map2.get(column2), map1.get(column1))) {
                    result.add(joinUtil.mergeMaps(map1, map2, tableName1, tableName2));
                }
            }
        }

        return result;
    }



    private boolean compare(Operation predicate, String value, String s) {
        return Objects.equals(value, s);
    }
}
