package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Index;
import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.university.dbmsibackend.util.Mapper.mapToIndexFileValue;

@Service
public class WhereClauseService {
    @Autowired
    private JsonUtil jsonUtil;
    @Autowired
    private MongoService mongoService;
    private String databaseName;

    public List<Map<String, Object>> handleWhereClause(List<Map<String, Object>> rows, Expression expression, String databaseName) {
        this.databaseName = databaseName;
        switch (expression.getClass().getSimpleName()) {
            case "EqualsTo" -> {
                System.out.println("Handling EqualsTo: " + expression);
                EqualsTo equalsTo = (EqualsTo) expression;
                Expression leftExpression = equalsTo.getLeftExpression();
                Expression rightExpression = equalsTo.getRightExpression();
                if (leftExpression != null && rightExpression != null) {
                    String fieldName = leftExpression.toString().split("\\.")[1];
                    String tableName = leftExpression.toString().split("\\.")[0];
                    Object value = convertExpressionToCorrectType(rightExpression);

                    List<IndexFileValue> indexFileValues = hasIndexFile(fieldName, tableName);

                    rows = rows.stream().filter(row -> {
                        var rowField = row.get(fieldName);
                        return rowField.equals(value);
                    }).toList();

                   // indexFileValues.stream().filter(indexFileValue -> indexFileValue==value);
                }
            }
            case "GreaterThan" -> System.out.println("Handling GreaterThan: " + expression);
            case "AndExpression" -> {
                System.out.println("Handling AND expression:");
                AndExpression andExpression = (AndExpression) expression;
                rows = handleWhereClause(rows, andExpression.getLeftExpression(), databaseName);
                rows = handleWhereClause(rows, andExpression.getRightExpression(), databaseName);
            }
            case "OrExpression" -> {
                System.out.println("Handling OR expression:");
                OrExpression orExpression = (OrExpression) expression;
                rows = handleWhereClause(rows, orExpression.getLeftExpression(), databaseName);
                rows = handleWhereClause(rows, orExpression.getRightExpression(), databaseName);
            }
            // Add more cases for other types of conditions as needed

            default -> System.out.println("Unhandled condition type: " + expression.getClass().getSimpleName());
        }

        return rows;
    }

    private List<IndexFileValue> hasIndexFile(String fieldName, String tableName) {
        List<IndexFileValue> indexFileValues = new ArrayList<>();
        Table table = jsonUtil.getTable(tableName, databaseName);
        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            List<String> attributeNames = index.getAttributes().stream().map(Attribute::getName).toList();
            if (attributeNames.contains(fieldName)) {
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
            return (Integer) (int) ((LongValue) expression).getValue();
        } else if (expression instanceof DoubleValue) {
            return (Float) (float) ((DoubleValue) expression).getValue();
        } else if (expression instanceof StringValue) {
            return (String) ((StringValue) expression).getValue();
        } else if (expression instanceof NullValue) {
            return null;
        } else {
            // Handle other data types or use a default conversion based on your needs
            return expression.toString();
        }
    }
}
