package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Index;
import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.service.api.WhereClauseService;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import com.university.dbmsibackend.util.Util;
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
public class WhereClauseServiceImpl implements WhereClauseService {
    @Autowired
    private JsonUtil jsonUtil;
    @Autowired
    private MongoService mongoService;

    @Override
    public List<Map<String, String>> handleWhereClause(Expression whereExpression, String databaseName, List<Map<String, String>> rows) {
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
            rows = rows.stream()
                    .filter(map -> {
                        if (Util.canConvertToInt(map.get(leftExpression.toString()))) {
                            int intValue = Integer.parseInt(map.get(leftExpression.toString()));
                            int intValueFromCondition = Integer.parseInt(rightExpression.toString());
                            return intValue > intValueFromCondition;
                        } else {
                            return map.get(leftExpression.toString()).compareTo(rightExpression.toString()) > 0;
                        }
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
        System.out.println("handling equals");
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
}
