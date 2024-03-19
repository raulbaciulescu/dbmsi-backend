package com.university.dbmsibackend.service.api;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.List;
import java.util.Map;

public interface GroupByService {
    Map<List<String>, List<Map<String, String>>> doGroupBy(List<String> groupByList, List<Map<String, String>> rows);

    List<Map<String, String>> filterRowsGroupBy(PlainSelect plainSelect, Map<List<String>, List<Map<String, String>>> rowsAfterGroupBy, List<String> groupByList);

    List<Map<String, String>> handleHaving(Expression havingExpression, List<Map<String, String>> rows);
}
