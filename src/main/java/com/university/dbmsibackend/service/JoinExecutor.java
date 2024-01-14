package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.Operation;
import com.university.dbmsibackend.service.api.JoinService;
import lombok.AllArgsConstructor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.Join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@AllArgsConstructor
public
class JoinExecutor {
    private final JoinService joinService;

    public List<Map<String, String>> executeJoin(List<Join> joins, String databaseName) {
        List<Map<String, String>> rows = new ArrayList<>();
        for (Join join : joins) {
            String tableName1 = "", tableName2 = "", column1 = "", column2 = "";
            Expression expression = join.getOnExpression();
            if (expression instanceof EqualsTo equalsTo) {
                Expression leftExpression = equalsTo.getLeftExpression();
                Expression rightExpression = equalsTo.getRightExpression();
                String leftParameter = leftExpression.toString();
                String rightParameter = rightExpression.toString();

                tableName1 = Arrays.stream(leftParameter.split("\\.")).toList().get(0);
                column1 = Arrays.stream(leftParameter.split("\\.")).toList().get(1);

                tableName2 = Arrays.stream(rightParameter.split("\\.")).toList().get(0);
                column2 = Arrays.stream(rightParameter.split("\\.")).toList().get(1);
            }
            if (rows.isEmpty()) {
                    rows = joinService.doJoin(
                            tableName1,
                            tableName2,
                            column1,
                            column2,
                            databaseName,
                            Operation.EQUALS
                    );
            } else
                rows = joinService.secondJoin(
                        rows,
                        tableName1,
                        tableName2,
                        column1,
                        column2,
                        databaseName,
                        Operation.EQUALS
                );
        }

        return rows;
    }
}
