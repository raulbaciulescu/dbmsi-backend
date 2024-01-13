package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.exception.SelectQueryException;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import com.university.dbmsibackend.util.TableMapper;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.Join;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class JoinService {
    private String databaseName;
    @Autowired
    private JsonUtil jsonUtil;
    @Autowired
    private MongoService mongoService;

    public List<Map<String, String>> handleJoin(Map<String, List<Map<String, String>>> map, List<Join> joins, String databaseName) {
        this.databaseName = databaseName;
        List<Map<String, String>> resultOfJoins = new ArrayList<>();
        for (Join join : joins) {
            Expression expression = join.getOnExpression();
            if (resultOfJoins.isEmpty())
                resultOfJoins = firstJoin(map, expression);
            else
                resultOfJoins = secondJoin(map, resultOfJoins, expression);
            System.out.println("Result: " + resultOfJoins);
        }
        return resultOfJoins;
    }

    private List<Map<String, String>> secondJoin(Map<String, List<Map<String, String>>> map, List<Map<String, String>> firstJoinList, Expression expression) {
        List<Map<String, String>> commonList = new ArrayList<>();
        if (expression instanceof EqualsTo equalsTo) {
            Expression leftExpression = equalsTo.getLeftExpression();
            Expression rightExpression = equalsTo.getRightExpression();
            String leftParameter = leftExpression.toString();
            String rightParameter = rightExpression.toString();

            String tableName1 = Arrays.stream(leftParameter.split("\\.")).toList().get(0);
            String column1 = Arrays.stream(leftParameter.split("\\.")).toList().get(1);

            String tableName2 = Arrays.stream(rightParameter.split("\\.")).toList().get(0);
            String column2 = Arrays.stream(rightParameter.split("\\.")).toList().get(1);

            List<Map<String, String>> table1RowsJsons = getTableJsonList(leftParameter, map);
            List<Map<String, String>> table2RowsJsons = getTableJsonList(rightParameter, map);

            for (Map<String, String> json1 : firstJoinList) {
                if (json1.containsKey(tableName1 + "." + column1)) {
                    var result = table2RowsJsons
                            .stream()
                            .filter(json2 -> Objects.equals(json2.get(column2), json1.get(tableName1 + "." + column1)))
                            .toList();
                    if (!result.isEmpty()) {
                        for (Map<String, String> json2 : result) {
                            Map<String, String> jsonResult = new HashMap<>();
                            for (String key : json1.keySet()) {
                                jsonResult.put(key, json1.get(key));
                            }
                            for (String key : json2.keySet()) {
                                jsonResult.put(tableName2 + "." + key, json2.get(key));
                            }
                            commonList.add(jsonResult);
                        }
                    }
                } else {
                    var result = table1RowsJsons
                            .stream()
                            .filter(json2 -> Objects.equals(json2.get(column1), json1.get(tableName2 + "." + column2)))
                            .toList();
                    if (!result.isEmpty()) {
                        for (Map<String, String> json2 : result) {
                            Map<String, String> jsonResult = new HashMap<>();
                            for (String key : json1.keySet()) {
                                jsonResult.put(tableName1 + "." + key, json1.get(key));
                            }
                            for (String key : json2.keySet()) {
                                jsonResult.put(key, json2.get(key));
                            }
                            commonList.add(jsonResult);
                        }
                    }
                }
            }
        }
        return commonList;
    }

    private List<Map<String, String>> firstJoin(Map<String, List<Map<String, String>>> map, Expression expression) {
        if (expression instanceof EqualsTo equalsTo) {
            Expression leftExpression = equalsTo.getLeftExpression();
            Expression rightExpression = equalsTo.getRightExpression();
            String leftParameter = leftExpression.toString();
            String rightParameter = rightExpression.toString();

            String tableName1 = Arrays.stream(leftParameter.split("\\.")).toList().get(0);
            String column1 = Arrays.stream(leftParameter.split("\\.")).toList().get(1);
            String tableName2 = Arrays.stream(rightParameter.split("\\.")).toList().get(0);
            String column2 = Arrays.stream(rightParameter.split("\\.")).toList().get(1);

            List<Map<String, String>> commonList = new ArrayList<>();
            List<Map<String, String>> table1RowsJsons = getTableJsonList(leftParameter, map);
            List<Map<String, String>> table2RowsJsons = getTableJsonList(rightParameter, map);

            for (Map<String, String> json1 : table1RowsJsons) {
                var result = table2RowsJsons
                        .stream()
                        .filter(json2 -> Objects.equals(json2.get(column2), json1.get(column1)))
                        .toList();
                if (!result.isEmpty()) {
                    for (Map<String, String> json2 : result) {
                        Map<String, String> jsonResult = new HashMap<>();
                        for (String key : json1.keySet()) {
                            jsonResult.put(tableName1 + "." + key, json1.get(key));
                        }
                        for (String key : json2.keySet()) {
                            jsonResult.put(tableName2 + "." + key, json2.get(key));
                        }
                        commonList.add(jsonResult);
                    }
                }
            }
            return commonList;
        } else
            throw new SelectQueryException("trebuie sa fie egal!");
    }

    public List<Map<String, String>> getTableJsonList(String parameter, Map<String, List<Map<String, String>>> map) {
        String tableName = Arrays.stream(parameter.split("\\.")).toList().get(0);

        Table table1 = jsonUtil.getTable(tableName, databaseName);
        List<SelectAllResponse> table1Rows;
        List<Map<String, String>> tableRowsJsons;

        if (map.containsKey(tableName) && !map.get(tableName).isEmpty())
            tableRowsJsons = map.get(tableName);
        else {
            table1Rows = mongoService.selectAll(databaseName, tableName);
            tableRowsJsons = table1Rows
                    .stream()
                    .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table1)))
                    .toList();
        }

        return tableRowsJsons;
    }
}
