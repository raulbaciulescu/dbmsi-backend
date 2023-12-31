package com.university.dbmsibackend.service;

import com.mongodb.client.MongoClient;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.QueryRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.TableMapper;
import lombok.AllArgsConstructor;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.util.*;

@Service
public class QueryService {
    @Autowired
    private MongoClient mongoClient;
    @Autowired
    private MongoService mongoService;
    @Autowired
    private JsonUtil jsonUtil;
    private String databaseName;

    public List<Map<String, Object>> executeQuery(QueryRequest request) {
        System.out.println(request.query());
        String query = request.query();
        databaseName = request.databaseName();
        try {
            // Parse the SQL query
            Statement statement = CCJSqlParserUtil.parse(query);

            // Print the traversable tree
            return printSqlTree(databaseName, statement);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private List<Map<String, Object>> printSqlTree(String databaseName, Statement statement) throws Exception {
        if (statement instanceof Select) {
            Select select = (Select) statement;
            Select selectBody = select.getSelectBody();
            return printSelectBodyTree(databaseName, selectBody);
        } else {
            System.out.println("Not a SELECT statement.");
        }
        return null;
    }

    private List<Map<String, Object>> printSelectBodyTree(String databaseName, Select selectBody) throws Exception {
        String fromTableName;

        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            fromTableName = plainSelect.getFromItem().toString();
            Table table = jsonUtil.getTable(fromTableName, databaseName);

//            creare lista totala si parsare randuri
            List<SelectAllResponse> rows = mongoService.selectAll(databaseName, fromTableName);
            List<Map<String,Object>> result = new ArrayList<>();
            for (SelectAllResponse row: rows) {
                Map<String, Object> jsonRow = mapRow(row.key(), row.value(), table);
                result.add(jsonRow);
            }

            var whereExpression = plainSelect.getWhere();
            if (whereExpression != null) {
                result = handleWhereClause(result, whereExpression);
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
            result = selectSimpleQuery(result, selectedItems);

            return result;
        } else throw new Exception("DA");
    }

    private static Object convertStringToCorrectType(String type, String value) {
        switch (type){
            case "varchar":
                return value;
            case "integer":
                return Integer.parseInt(value);
            case "float":
                return Float.parseFloat(value);
            case "bool":
                return Boolean.parseBoolean(value);
            default:
                return null;
        }
    }

    private Map<String, Object> mapRow(String key, String value, Table table) {
        String[] primaryKeys = key.split("#", -1);
        String[] values = value.split("#", -1);

        Map<String, Object> result = new HashMap<>();

        int primaryKeysIndex = 0, valuesIndex = 0;
        for (Attribute attribute : table.getAttributes()) {
            if (table.getPrimaryKeys().contains(attribute.getName())) {
                result.put(attribute.getName(),
                        convertStringToCorrectType(attribute.getType(),primaryKeys[primaryKeysIndex]));
                primaryKeysIndex++;
            } else {
                result.put(attribute.getName(),
                        convertStringToCorrectType(attribute.getType(), values[valuesIndex]));
                valuesIndex++;
            }
        }

        return result;
    }

    private List<Map<String, Object>> handleWhereClause(List<Map<String, Object>> rows, Expression expression) {
        switch (expression.getClass().getSimpleName()) {
            case "EqualsTo":
                System.out.println("Handling EqualsTo: " + expression);
                EqualsTo equalsTo = (EqualsTo) expression;
                Expression leftExpression = equalsTo.getLeftExpression();
                Expression rightExpression = equalsTo.getRightExpression();

                if (leftExpression != null && rightExpression != null) {
                    String fieldName = leftExpression.toString().split("\\.")[1];
                    Object value = convertExpressionToCorrectType(rightExpression);
                    rows = rows.stream().filter(row -> {
                        var rowField = row.get(fieldName);
                        return rowField.equals(value);
                    }).toList();
                }
                break;
            case "GreaterThan":
                System.out.println("Handling GreaterThan: " + expression);
                // Handle GreaterThan condition
                break;
            case "AndExpression":
                System.out.println("Handling AND expression:");
                AndExpression andExpression = (AndExpression) expression;
                rows = handleWhereClause(rows,andExpression.getLeftExpression());
                rows = handleWhereClause(rows,andExpression.getRightExpression());
                break;
            case "OrExpression":
                System.out.println("Handling OR expression:");
                OrExpression orExpression = (OrExpression) expression;
                rows = handleWhereClause(rows, orExpression.getLeftExpression());
                rows = handleWhereClause(rows, orExpression.getRightExpression());
                break;
            // Add more cases for other types of conditions as needed

            default:
                System.out.println("Unhandled condition type: " + expression.getClass().getSimpleName());
                break;
        }

        return rows;
    }

    private Object convertExpressionToCorrectType(Expression expression) {
        if (expression instanceof LongValue) {
            return (Integer)(int)((LongValue) expression).getValue();
        } else if (expression instanceof DoubleValue) {
            return (Float)(float)((DoubleValue) expression).getValue();
        } else if (expression instanceof StringValue) {
            return (String)((StringValue) expression).getValue();
        } else if (expression instanceof NullValue) {
            return null;
        } else {
            // Handle other data types or use a default conversion based on your needs
            return expression.toString();
        }
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
                    .map(s -> dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table1)))
                    .toList();
            List<Map<String, String>> table2RowsJsons = table2Rows
                    .stream()
                    .map(s -> dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table2)))
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

    private List<Map<String, String>> firstJoin(Expression expression) throws Exception {
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
                        .map(s -> dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table1)))
                        .toList();
                List<Map<String, String>> table2RowsJsons = table2Rows
                        .stream()
                        .map(s -> dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table2)))
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
            throw new Exception("trebuie sa fie egal!");
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
     * @param fromTableName
     * @param selectItems   [name, age]
     * @return
     */
    private List<Map<String, Object>> selectSimpleQuery(List<Map<String, Object>> rows, List<String> selectItems) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row: rows) {
            Map<String, Object> resultJson = new HashMap<>();
            for (String key : row.keySet()) {
                if (selectItems.contains(key))
                    resultJson.put(key, row.get(key)); //{"name": "raul", "age": 21}
            }
            result.add(resultJson);
        }

        return result;
    }

    private static <K, V> Map<K, V> dictionaryToMap(Dictionary<K, V> dictionary) {
        Map<K, V> map = new HashMap<>();

        // Iterate over the keys in the Dictionary and add them to the Map
        Enumeration<K> keys = dictionary.keys();
        while (keys.hasMoreElements()) {
            K key = keys.nextElement();
            V value = dictionary.get(key);
            map.put(key, value);
        }

        return map;
    }
}
