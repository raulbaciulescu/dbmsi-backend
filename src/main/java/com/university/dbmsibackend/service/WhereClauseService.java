package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Index;
import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import com.university.dbmsibackend.util.TableMapper;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class WhereClauseService {
    @Autowired
    private JsonUtil jsonUtil;
    @Autowired
    private MongoService mongoService;
    private String databaseName;

    public Map<String, List<Dictionary<String, String>>> handleWhereClause(Expression whereExpression, String databaseName) {
        Map<String, List<Dictionary<String, String>>> tableNamePrimaryKeysMap = new HashMap<>();
        this.databaseName = databaseName;
        tableNamePrimaryKeysMap = handleWhereClause(tableNamePrimaryKeysMap, whereExpression);

        return tableNamePrimaryKeysMap;
    }

    public Map<String, List<Dictionary<String, String>>> handleWhereClause(Map<String, List<Dictionary<String, String>>> tableNamePrimaryKeysMap, Expression expression) {
        switch (expression.getClass().getSimpleName()) {
            case "EqualsTo" -> handleEqualsTo(tableNamePrimaryKeysMap, expression);
            case "GreaterThan" -> handleGreaterThan(tableNamePrimaryKeysMap, expression);
            case "AndExpression" -> {
                System.out.println("Handling AND expression:");
                AndExpression andExpression = (AndExpression) expression;
                tableNamePrimaryKeysMap = handleWhereClause(tableNamePrimaryKeysMap, andExpression.getLeftExpression());
                tableNamePrimaryKeysMap = handleWhereClause(tableNamePrimaryKeysMap, andExpression.getRightExpression());
            }
            default -> System.out.println("Unhandled condition type: " + expression.getClass().getSimpleName());
        }

        return tableNamePrimaryKeysMap;
    }

    private void handleGreaterThan(Map<String, List<Dictionary<String, String>>> tableNamePrimaryKeysMap, Expression expression) {
        System.out.println("Handling GreaterThan: " + expression);
        GreaterThan equalsTo = (GreaterThan) expression;
        Expression leftExpression = equalsTo.getLeftExpression();
        Expression rightExpression = equalsTo.getRightExpression();
        if (leftExpression != null && rightExpression != null) {
            String fieldName = leftExpression.toString().split("\\.")[1];
            String tableName = leftExpression.toString().split("\\.")[0];

            List<IndexFileValue> indexFileValues = hasIndexFile(fieldName, tableName);
            if (!indexFileValues.isEmpty()) {
                System.out.println("am gasit index pentru " + tableName + "." + fieldName);
                List<List<String>> listOfLists = indexFileValues
                        .stream()
                        //.filter(indexFileValue -> Objects.e quals(indexFileValue.value(), rightExpression.toString()))
                        .filter(indexFileValue -> {
                            int intValue = Integer.parseInt(indexFileValue.value());
                            int intValueFromCondition = Integer.parseInt(rightExpression.toString());
                            System.out.println("intValue " + intValue);
                            System.out.println("intValueFromCondition" + intValueFromCondition);
                            return intValue > intValueFromCondition;
                        })
                        .map(IndexFileValue::primaryKeys)
                        .toList();
                List<String> listOfPrimaryKeys = listOfLists.stream()
                        .flatMap(List::stream)
                        .distinct()
                        .toList();
                List<SelectAllResponse> newList = mongoService.getByPrimaryKeys(databaseName, tableName, listOfPrimaryKeys);
                List<Dictionary<String, String>> newDictionaryList = TableMapper.mapKeyValueListToTableRow(newList, jsonUtil.getTable(tableName, databaseName));
                List<Dictionary<String, String>> existingList = tableNamePrimaryKeysMap.get(tableName);
                System.out.println(tableName + "." + fieldName + " newList " + newList);
                System.out.println(tableName + "." + fieldName + " existing " + existingList);
                System.out.println(tableName + "." + fieldName + " newDictionaryList " + newDictionaryList);
                if (tableNamePrimaryKeysMap.containsKey(tableName)) {
                    existingList = intersectLists(existingList, newDictionaryList); // intersection
                } else {
                    existingList = newDictionaryList;
                }
                System.out.println(tableName + "." + fieldName + " after intersection " + existingList);
                tableNamePrimaryKeysMap.put(tableName, existingList);
            } else { // we don't have an index for fieldName
                List<SelectAllResponse> rows = mongoService.selectAll(databaseName, tableName);
                List<Dictionary<String, String>> dictionaries = TableMapper.mapKeyValueListToTableRow(rows, jsonUtil.getTable(tableName, databaseName));
                List<Dictionary<String, String>> filteredList = dictionaries
                        .stream()
                        //.filter(el -> Objects.equals(el.get(fieldName), rightExpression.toString()))
                        .filter(el -> {
                            int intValue = Integer.parseInt(el.get(fieldName));
                            int intValueFromCondition = Integer.parseInt(rightExpression.toString());
                            System.out.println("intValue " + intValue);
                            System.out.println("intValueFromCondition" + intValueFromCondition);
                            return intValue > intValueFromCondition;
                        })
                        .toList();
                System.out.println("filtered list: " + filteredList);
                List<Dictionary<String, String>> existingList = tableNamePrimaryKeysMap.get(tableName);
                if (tableNamePrimaryKeysMap.containsKey(tableName)) {
                    existingList = intersectLists(existingList, filteredList);
                } else {
                    existingList = filteredList;
                }
                tableNamePrimaryKeysMap.put(tableName, existingList);
            }
        }
    }

    /**
     * Map<String, List<Dictionary<String, String>>>
     *     key = tableName
     *     list = rows
     *     group.id = da AND student.id = da
     * @param tableNamePrimaryKeysMap
     * @param expression
     */
    private void handleEqualsTo(Map<String, List<Dictionary<String, String>>> tableNamePrimaryKeysMap, Expression expression) {
        System.out.println("Handling EqualsTo: " + expression);
        EqualsTo equalsTo = (EqualsTo) expression;
        Expression leftExpression = equalsTo.getLeftExpression();
        Expression rightExpression = equalsTo.getRightExpression();
        if (leftExpression != null && rightExpression != null) {
            String fieldName = leftExpression.toString().split("\\.")[1];
            String tableName = leftExpression.toString().split("\\.")[0];

            List<IndexFileValue> indexFileValues = hasIndexFile(fieldName, tableName);
            if (!indexFileValues.isEmpty()) {
                System.out.println("am gasit index pentru " + tableName + "." + fieldName);
                List<List<String>> listOfLists = indexFileValues
                        .stream()
                        .filter(indexFileValue -> Objects.equals(indexFileValue.value(), rightExpression.toString()))
                        .map(IndexFileValue::primaryKeys)
                        .toList();
                List<String> listOfPrimaryKeys = listOfLists.stream()
                        .flatMap(List::stream)
                        .distinct()
                        .toList();
                List<SelectAllResponse> newList = mongoService.getByPrimaryKeys(databaseName, tableName, listOfPrimaryKeys);
                List<Dictionary<String, String>> newDictionaryList = TableMapper.mapKeyValueListToTableRow(newList, jsonUtil.getTable(tableName, databaseName));
                List<Dictionary<String, String>> existingList = tableNamePrimaryKeysMap.get(tableName);
                System.out.println(tableName + "." + fieldName + " newList " + newList);
                System.out.println(tableName + "." + fieldName + " existing " + existingList);
                System.out.println(tableName + "." + fieldName + " newDictionaryList " + newDictionaryList);
                if (tableNamePrimaryKeysMap.containsKey(tableName)) {
                    existingList = intersectLists(existingList, newDictionaryList); // intersection
                } else {
                    existingList = newDictionaryList;
                }
                System.out.println(tableName + "." + fieldName + " after intersection " + existingList);
                tableNamePrimaryKeysMap.put(tableName, existingList);
            } else { // we don't have an index for fieldName
                List<SelectAllResponse> rows = mongoService.selectAll(databaseName, tableName);
                List<Dictionary<String, String>> dictionaries = TableMapper.mapKeyValueListToTableRow(rows, jsonUtil.getTable(tableName, databaseName));
                List<Dictionary<String, String>> filteredList = dictionaries
                        .stream()
                        .filter(el -> Objects.equals(el.get(fieldName), rightExpression.toString()))
                        .toList();
                System.out.println("filtered list: " + filteredList);
                List<Dictionary<String, String>> existingList = tableNamePrimaryKeysMap.get(tableName);
                if (tableNamePrimaryKeysMap.containsKey(tableName)) {
                    existingList = intersectLists(existingList, filteredList);
                } else {
                    existingList = filteredList;
                }
                tableNamePrimaryKeysMap.put(tableName, existingList);
            }
        }
    }

    private List<Dictionary<String, String>> intersectLists(List<Dictionary<String, String>> existingList, List<Dictionary<String, String>> newDictionaryList) {
        List<Dictionary<String, String>> result = new ArrayList<>();

        for (Dictionary<String, String> existingDict : existingList) {
            for (Dictionary<String, String> newDict : newDictionaryList) {
                if (existingDict.equals(newDict)) {
                    result.add(existingDict);
                    break;
                }
            }
        }

        return result;
    }

    private List<IndexFileValue> hasIndexFile(String fieldName, String tableName) {
        List<IndexFileValue> indexFileValues = new ArrayList<>();
        boolean hasIndex = true;
        Table table = jsonUtil.getTable(tableName, databaseName);
        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            List<String> attributeNames = index.getAttributes().stream().map(Attribute::getName).toList();
            if (attributeNames.contains(fieldName)) {
                hasIndex = false;
                MongoDatabase database = mongoService.getDatabase(databaseName);
                MongoCollection<Document> collection = database.getCollection(tableName + "_" + index.getName() + ".ind");
                FindIterable<Document> documents = collection.find();
                indexFileValues = Mapper.mapToIndexFileValue(documents);
            }
        }

        return indexFileValues;
    }

    private Object convertExpressionToCorrectType(Expression expression) {
        if (expression instanceof LongValue) {
            return (int) ((LongValue) expression).getValue();
        } else if (expression instanceof DoubleValue) {
            return (float) ((DoubleValue) expression).getValue();
        } else if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof NullValue) {
            return null;
        } else {
            // Handle other data types or use a default conversion based on your needs
            return expression.toString();
        }
    }
}
