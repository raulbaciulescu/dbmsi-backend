package com.university.dbmsibackend.service.api;

import net.sf.jsqlparser.expression.Expression;

import java.util.List;
import java.util.Map;

public interface WhereClauseService {
    List<Map<String, String>> handleWhereClause(Expression whereExpression, String databaseName, List<Map<String, String>> rows);
}
