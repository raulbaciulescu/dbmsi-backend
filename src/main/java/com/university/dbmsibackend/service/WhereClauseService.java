package com.university.dbmsibackend.service;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class WhereClauseService {
    public List<Map<String, Object>> handleWhereClause(List<Map<String, Object>> rows, Expression expression) {
        switch (expression.getClass().getSimpleName()) {
            case "EqualsTo" -> {
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
            }
            case "GreaterThan" -> System.out.println("Handling GreaterThan: " + expression);
            case "AndExpression" -> {
                System.out.println("Handling AND expression:");
                AndExpression andExpression = (AndExpression) expression;
                rows = handleWhereClause(rows, andExpression.getLeftExpression());
                rows = handleWhereClause(rows, andExpression.getRightExpression());
            }
            case "OrExpression" -> {
                System.out.println("Handling OR expression:");
                OrExpression orExpression = (OrExpression) expression;
                rows = handleWhereClause(rows, orExpression.getLeftExpression());
                rows = handleWhereClause(rows, orExpression.getRightExpression());
            }
            // Add more cases for other types of conditions as needed

            default -> System.out.println("Unhandled condition type: " + expression.getClass().getSimpleName());
        }

        return rows;
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
