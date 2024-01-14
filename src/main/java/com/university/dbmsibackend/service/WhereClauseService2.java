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
import java.util.stream.Collectors;

@Service
public class WhereClauseService2 {
    @Autowired
    private JsonUtil jsonUtil;
    @Autowired
    private MongoService mongoService;
    private String databaseName;

    public List<Map<String, String>> handleWhereClause(Expression whereExpression, String databaseName, List<Map<String, String>> rows) {
        this.databaseName = databaseName;
        rows = handleWhereClause(rows, whereExpression);

        return rows;
    }

    private List<Map<String, String>> handleWhereClause(List<Map<String, String>> rows, Expression expression) {
        switch (expression.getClass().getSimpleName()) {
            case "EqualsTo" -> rows = handleEqualsTo(rows, expression);
            case "GreaterThan" -> rows = handleGreaterThan(rows, expression);
            case "AndExpression" -> {
                System.out.println("Handling AND expression:");
                AndExpression andExpression = (AndExpression) expression;
                rows = handleWhereClause(rows, andExpression.getLeftExpression());
                rows = handleWhereClause(rows, andExpression.getRightExpression());
            }
            default -> System.out.println("Unhandled condition type: " + expression.getClass().getSimpleName());
        }

        return rows;
    }

    private List<Map<String, String>> handleGreaterThan(List<Map<String, String>> rows, Expression expression) {
        System.out.println("Handling GreaterThan: " + expression);
        GreaterThan equalsTo = (GreaterThan) expression;
        Expression leftExpression = equalsTo.getLeftExpression();
        Expression rightExpression = equalsTo.getRightExpression();
        if (leftExpression != null && rightExpression != null) {
            String fieldName = leftExpression.toString().split("\\.")[1];
            String tableName = leftExpression.toString().split("\\.")[0];

            rows = rows.stream()
                    .filter(map -> {
                        int intValue = Integer.parseInt(map.get(leftExpression.toString()));
                        int intValueFromCondition = Integer.parseInt(rightExpression.toString());

                        System.out.println("int1: " + intValue);
                        System.out.println("int2 " + intValueFromCondition);
                        return intValue > intValueFromCondition;
                    })
                    .collect(Collectors.toList());
        }
        return rows;
    }

    /**
     * Map<String, List<Dictionary<String, String>>>
     * key = tableName
     * list = rows
     * group.id = da AND student.id = da
     *
     * @param rows
     * @param expression
     */
    private List<Map<String, String>> handleEqualsTo(List<Map<String, String>> rows, Expression expression) {
        EqualsTo equalsTo = (EqualsTo) expression;
        Expression leftExpression = equalsTo.getLeftExpression();
        Expression rightExpression = equalsTo.getRightExpression();
        if (leftExpression != null && rightExpression != null) {
            rows = rows.stream()
                    .filter(map -> Objects.equals(map.get(leftExpression.toString()), rightExpression.toString()))
                    .collect(Collectors.toList());
            System.out.println("rows " + rows);
        }
        return rows;
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
