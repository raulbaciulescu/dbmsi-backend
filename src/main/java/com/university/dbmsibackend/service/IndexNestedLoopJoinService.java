package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Operation;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.service.api.JoinService;
import com.university.dbmsibackend.util.JoinUtil;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.Join;
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
        boolean hasIndex1 = jsonUtil.hasIndex(tableName1, column1, databaseName);
        boolean hasIndex2 = jsonUtil.hasIndex(tableName2, column2, databaseName);
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

        return result;
    }

    private List<Map<String, String>> simpleNestedLoop(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        List<Map<String, String>> table1RowsJsons = mongoService.getTableJsonList(tableName1, databaseName);
        List<Map<String, String>> table2RowsJsons = mongoService.getTableJsonList(tableName2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();

        for (Map<String, String> map1 : table1RowsJsons) {
            for (Map<String, String> map2 : table2RowsJsons) {
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

    public List<Map<String, String>> doJoin(List<Join> joins, String databaseName) {
        List<Map<String, String>> rows = new ArrayList<>();
        for (Join join : joins) {
            Expression expression = join.getOnExpression();
            if (rows.isEmpty()) {

                if (expression instanceof EqualsTo equalsTo) {
                    Expression leftExpression = equalsTo.getLeftExpression();
                    Expression rightExpression = equalsTo.getRightExpression();
                    String leftParameter = leftExpression.toString();
                    String rightParameter = rightExpression.toString();

                    String tableName1 = Arrays.stream(leftParameter.split("\\.")).toList().get(0);
                    String column1 = Arrays.stream(leftParameter.split("\\.")).toList().get(1);

                    String tableName2 = Arrays.stream(rightParameter.split("\\.")).toList().get(0);
                    String column2 = Arrays.stream(rightParameter.split("\\.")).toList().get(1);
                    rows = doJoin(
                            tableName1,
                            tableName2,
                            column1,
                            column2,
                            databaseName,
                            Operation.EQUALS
                    );
                }
            }
            System.out.println("Result: " + rows);
        }
        return rows;
    }
}
