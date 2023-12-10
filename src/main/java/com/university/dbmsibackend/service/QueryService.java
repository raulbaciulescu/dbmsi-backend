package com.university.dbmsibackend.service;

import com.mongodb.client.MongoClient;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.QueryRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.exception.SelectQueryException;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import com.university.dbmsibackend.util.TableMapper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.springframework.stereotype.Service;


import java.util.*;

@Service
public class QueryService {
    private final MongoClient mongoClient;
    private final MongoService mongoService;
    private final WhereClauseService whereClauseService;
    private final JsonUtil jsonUtil;
    private String databaseName;

    public QueryService(JsonUtil jsonUtil, MongoClient mongoClient, MongoService mongoService, WhereClauseService whereClauseService) {
        this.jsonUtil = jsonUtil;
        this.mongoClient = mongoClient;
        this.mongoService = mongoService;
        this.whereClauseService = whereClauseService;
        this.databaseName = "";
    }

    public List<Map<String, Object>> executeQuery(QueryRequest request) {
        String query = request.query();
        databaseName = request.databaseName();
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            return processSqlTree(statement);
        } catch (JSQLParserException e) {
            throw new SelectQueryException(e.getMessage());
        }
    }

    private List<Map<String, Object>> processSqlTree(Statement statement) {
        if (statement instanceof Select select) {
            Select selectBody = select.getSelectBody();
            return processSelectBodyTree(selectBody);
        } else
            throw new SelectQueryException("Not a SELECT statement.");
    }

    private List<Map<String, Object>> processSelectBodyTree(Select selectBody) {
        String fromTableName;

        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            fromTableName = plainSelect.getFromItem().toString();
            Table table = jsonUtil.getTable(fromTableName, databaseName);

//            creare lista totala si parsare randuri
            List<SelectAllResponse> rows = mongoService.selectAll(databaseName, fromTableName);
            List<Map<String, Object>> result = new ArrayList<>();
            for (SelectAllResponse row : rows) {
                Map<String, Object> jsonRow = mapRow(row.key(), row.value(), table);
                result.add(jsonRow);
            }

            var whereExpression = plainSelect.getWhere();
            if (whereExpression != null) {
                result = whereClauseService.handleWhereClause(result, whereExpression);
            }

            // Print JOINs
            List<Map<String, String>> resultOfJoins = new ArrayList<>();
            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    String joinTableName = join.getRightItem().toString();
                    Expression expression = join.getOnExpression();
                    if (resultOfJoins.isEmpty())
                        resultOfJoins = firstJoin(expression);
                    else
                        resultOfJoins = secondJoin(resultOfJoins, expression);
                    System.out.println("Result: " + resultOfJoins);
                }
            }

            List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
            List<String> selectedItems = selectItems
                    .stream()
                    .map(SelectItem::toString)
                    .toList();
            result = filterSelectFields(result, selectedItems);

            return result;
        } else throw new SelectQueryException("DA");
    }

    private static Object convertStringToCorrectType(String type, String value) {
        return switch (type) {
            case "varchar" -> value;
            case "integer" -> Integer.parseInt(value);
            case "float" -> Float.parseFloat(value);
            case "bool" -> Boolean.parseBoolean(value);
            default -> null;
        };
    }

    private Map<String, Object> mapRow(String key, String value, Table table) {
        String[] primaryKeys = key.split("#", -1);
        String[] values = value.split("#", -1);

        Map<String, Object> result = new HashMap<>();

        int primaryKeysIndex = 0, valuesIndex = 0;
        for (Attribute attribute : table.getAttributes()) {
            if (table.getPrimaryKeys().contains(attribute.getName())) {
                result.put(attribute.getName(),
                        convertStringToCorrectType(attribute.getType(), primaryKeys[primaryKeysIndex]));
                primaryKeysIndex++;
            } else {
                result.put(attribute.getName(),
                        convertStringToCorrectType(attribute.getType(), values[valuesIndex]));
                valuesIndex++;
            }
        }

        return result;
    }

    private List<Map<String, String>> secondJoin(List<Map<String, String>> firstJoinList, Expression expression) {
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

            Table table1 = jsonUtil.getTable(tableName1, databaseName);
            Table table2 = jsonUtil.getTable(tableName2, databaseName);

            List<SelectAllResponse> table1Rows = mongoService.selectAll(databaseName, tableName1);
            List<SelectAllResponse> table2Rows = mongoService.selectAll(databaseName, tableName2);
            List<Map<String, String>> table1RowsJsons = table1Rows
                    .stream()
                    .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table1)))
                    .toList();
            List<Map<String, String>> table2RowsJsons = table2Rows
                    .stream()
                    .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table2)))
                    .toList();

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

    private List<Map<String, String>> firstJoin(Expression expression) {
        if (expression instanceof EqualsTo equalsTo) {
            Expression leftExpression = equalsTo.getLeftExpression();
            Expression rightExpression = equalsTo.getRightExpression();
            String leftParameter = leftExpression.toString();
            String rightParameter = rightExpression.toString();

            String tableName1 = Arrays.stream(leftParameter.split("\\.")).toList().get(0);
            String column1 = Arrays.stream(leftParameter.split("\\.")).toList().get(1);

            String tableName2 = Arrays.stream(rightParameter.split("\\.")).toList().get(0);
            String column2 = Arrays.stream(rightParameter.split("\\.")).toList().get(1);

            Table table1 = jsonUtil.getTable(tableName1, databaseName);
            Table table2 = jsonUtil.getTable(tableName2, databaseName);

            List<Map<String, String>> commonList = new ArrayList<>();

            // check if we have foreign key constraint
            if (!hasForeignKey()) {
                List<SelectAllResponse> table1Rows = mongoService.selectAll(databaseName, tableName1);
                List<SelectAllResponse> table2Rows = mongoService.selectAll(databaseName, tableName2);
                List<Map<String, String>> table1RowsJsons = table1Rows
                        .stream()
                        .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table1)))
                        .toList();
                List<Map<String, String>> table2RowsJsons = table2Rows
                        .stream()
                        .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table2)))
                        .toList();
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
                System.out.println("commonList " + commonList);
            }
            return commonList;
        } else
            throw new SelectQueryException("trebuie sa fie egal!");
    }

    private boolean hasForeignKey() {
        return false;
    }

    /**
     * {"id": 1, "groupId": 1, "name": "raul", "age": 21, "dadada": "DADADA"}
     * {"id": 1, "groupId": 1, "name": "raul", "age": 21, "dadada": "DADADA"}
     * {"id": 1, "groupId": 1, "name": "raul", "age": 21, "dadada": "DADADA"}
     * <p>
     * <p>
     * {"id": 1, "groupName": "da"}
     * {"id": 1, "groupName": "da"}
     *
     * @param selectItems   [name, age]
     * @return
     */
    private List<Map<String, Object>> filterSelectFields(List<Map<String, Object>> rows, List<String> selectItems) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> resultJson = new HashMap<>();
            for (String key : row.keySet()) {
                if (selectItems.contains(key) || selectItems.contains("*"))
                    resultJson.put(key, row.get(key)); //{"name": "raul", "age": 21}
            }
            result.add(resultJson);
        }

        return result;
    }
}
