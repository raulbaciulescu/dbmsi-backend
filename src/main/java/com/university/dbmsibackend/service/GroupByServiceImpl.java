package com.university.dbmsibackend.service;

import com.university.dbmsibackend.service.api.GroupByService;
import com.university.dbmsibackend.util.Util;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class GroupByServiceImpl implements GroupByService {
    @Override
    public Map<List<String>, List<Map<String, String>>> doGroupBy(List<String> groupByList, List<Map<String, String>> rows) {
        return rows.stream()
                .collect(Collectors.groupingBy(map ->
                        groupByList.stream()
                                .map(map::get)
                                .collect(Collectors.toList())
                ));
    }

    @Override
    public List<Map<String, String>> filterRowsGroupBy(PlainSelect plainSelect, Map<List<String>, List<Map<String, String>>> rowsAfterGroupBy, List<String> groupByList) {
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        List<Map<String, String>> rows = new ArrayList<>();
        List<String> operations = new ArrayList<>();
        List<String> operationsName = new ArrayList<>();

        for (SelectItem selectItem : selectItems) {
            Pattern pCount = Pattern.compile("^count\\((.+)\\)$");
            Pattern pMin = Pattern.compile("^min\\((.+)\\)$");
            Pattern pMax = Pattern.compile("^max\\((.+)\\)$");
            Pattern pSum = Pattern.compile("^sum\\((.+)\\)$");
            Matcher matcherCount = pCount.matcher(selectItem.toString());
            Matcher matcherMin = pMin.matcher(selectItem.toString());
            Matcher matcherMax = pMax.matcher(selectItem.toString());
            Matcher matcherSum = pSum.matcher(selectItem.toString());

            if (matcherCount.find()) {
                operations.add("count");
                operationsName.add(selectItem.toString());
            }
            if (matcherMin.find()) {
                operations.add("min^" + matcherMin.group(1));
                operationsName.add(selectItem.toString());
            }
            if (matcherMax.find()) {
                operations.add("max^" + matcherMax.group(1));
                operationsName.add(selectItem.toString());
            }
            if (matcherSum.find()) {
                operations.add("sum^" + matcherSum.group(1));
                operationsName.add(selectItem.toString());
            }
        }
        System.out.println(operations);
        System.out.println(operationsName);
        handleOperations(rows, rowsAfterGroupBy, groupByList, operations, operationsName);

        return rows;
    }

    @Override
    public List<Map<String, String>> handleHaving(Expression havingExpression, List<Map<String, String>> rows) {
        switch (havingExpression.getClass().getSimpleName()) {
            case "EqualsTo" -> rows = handleEqualsTo(rows, havingExpression);
            case "GreaterThan" -> rows = handleGreaterThan(rows, havingExpression);
            case "AndExpression" -> {
                System.out.println("Handling AND having expression:");
                AndExpression andExpression = (AndExpression) havingExpression;
                rows = handleHaving(andExpression.getLeftExpression(), rows);
                rows = handleHaving(andExpression.getRightExpression(), rows);
            }
            default -> System.out.println("Unhandled condition type: " + havingExpression.getClass().getSimpleName());
        }

        return rows;
    }

    private void handleOperations(List<Map<String, String>> rows, Map<List<String>,
            List<Map<String, String>>> rowsAfterGroupBy, List<String> groupByList, List<String> operationList, List<String> selectItems) {
        rowsAfterGroupBy.forEach((keyList, value) -> {
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < groupByList.size(); i++) {
                map.put(groupByList.get(i), keyList.get(i));
            }

            for (int i = 0; i < operationList.size(); i++) {
                if (operationList.get(i).matches("count")) {
                    map.put(selectItems.get(i), String.valueOf(value.size()));
                }
                if (operationList.get(i).matches("min.*")) {
                    map = handleMin(map, operationList.get(i), value, selectItems.get(i));
                }
                if (operationList.get(i).matches("max.*")) {
                    map = handleMax(map, operationList.get(i), value, selectItems.get(i));
                }
                if (operationList.get(i).matches("sum.*")) {
                    map = handleSum(map, operationList.get(i), value, selectItems.get(i));
                }
            }
            System.out.println(map);
            rows.add(map);
        });
        System.out.println(rows);
    }

    private Map<String, String> handleSum(Map<String, String> map, String operation, List<Map<String, String>> value, String string) {
        String sumKey = operation.split("\\^")[1];
        int sum = 0;
        for (Map<String, String> map2 : value) {
            if (Util.canConvertToInt(map2.get(sumKey))) {
                int intValue = Integer.parseInt(map2.get(sumKey));
                sum += intValue;
            }
        }
        map.put(string, String.valueOf(sum));

        return map;
    }

    private Map<String, String> handleMax(Map<String, String> map, String operation, List<Map<String, String>> value, String string) {
        String maxKey = operation.split("\\^")[1];
        int maxValue = -1;
        String maxString = "aaaaaaaaaaaaaa";
        for (Map<String, String> map2 : value) {
            if (Util.canConvertToInt(map2.get(maxKey))) {
                int intValue = Integer.parseInt(map2.get(maxKey));
                maxValue = Math.max(maxValue, intValue);
            } else if (map2.get(maxKey).compareTo(maxString) > 0) {
                maxString = map2.get(maxKey);
            }
        }
        if (maxValue != -1)
            map.put(string, String.valueOf(maxValue));
        else
            map.put(string, maxString);

        return map;
    }

    private Map<String, String> handleMin(Map<String, String> map, String operation, List<Map<String, String>> value, String string) {
        System.out.println("min " + string);
        String minKey = operation.split("\\^")[1];
        int minValue = 1_000_000;
        String minString = "zzzzzzzzzzzzz";
        for (Map<String, String> map2 : value) {
            if (Util.canConvertToInt(map2.get(minKey))) {
                int intValue = Integer.parseInt(map2.get(minKey));
                minValue = Math.min(minValue, intValue);
            } else if (map2.get(minKey).compareTo(minString) < 0) {
                minString = map2.get(minKey);
            }
        }
        if (minValue != 1_000_000)
            map.put(string, String.valueOf(minValue));
        else
            map.put(string, minString);

        return map;
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

    private List<Map<String, String>> handleEqualsTo(List<Map<String, String>> rows, Expression havingExpression) {
        EqualsTo equalsTo = (EqualsTo) havingExpression;
        Expression leftExpression = equalsTo.getLeftExpression();
        Expression rightExpression = equalsTo.getRightExpression();
        if (leftExpression != null && rightExpression != null) {
            rows = rows.stream()
                    .filter(map -> Objects.equals(map.get(leftExpression.toString()), rightExpression.toString()))
                    .collect(Collectors.toList());
            System.out.println("rows after having equals" + rows);
        }
        return rows;
    }
}
