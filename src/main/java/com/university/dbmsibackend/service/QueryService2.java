package com.university.dbmsibackend.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.QueryRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.exception.SelectQueryException;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import com.university.dbmsibackend.util.TableMapper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class QueryService2 {
    private final MongoClient mongoClient;
    private final MongoService mongoService;
    private final WhereClauseService2 whereClauseService;
    private final JoinServiceTemp joinServiceTemp;
    private final IndexNestedLoopJoinService indexNestedLoopJoinService;
    private final JsonUtil jsonUtil;
    private String databaseName;
    private final JoinExecutor joinExecutor;
    private final GroupByService groupByService;

    public QueryService2(JsonUtil jsonUtil,
                         MongoClient mongoClient,
                         MongoService mongoService,
                         WhereClauseService2 whereClauseService,
                         IndexNestedLoopJoinService indexNestedLoopJoinService,
                         GroupByService groupByService,
                         JoinServiceTemp joinServiceTemp,
                         JoinExecutor joinExecutor) {
        this.jsonUtil = jsonUtil;
        this.mongoClient = mongoClient;
        this.mongoService = mongoService;
        this.whereClauseService = whereClauseService;
        this.joinServiceTemp = joinServiceTemp;
        this.indexNestedLoopJoinService = indexNestedLoopJoinService;
        this.joinExecutor = joinExecutor;
        this.groupByService = groupByService;
        this.databaseName = "";
    }

    public List<Map<String, String>> executeQuery(QueryRequest request) {
        String query = request.query();
        databaseName = request.databaseName();
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            return processSqlTree(statement);
        } catch (JSQLParserException e) {
            throw new SelectQueryException(e.getMessage());
        }
    }

    private List<Map<String, String>> processSqlTree(Statement statement) {
        if (statement instanceof Select select) {
            Select selectBody = select.getSelectBody();
            return processSelectBodyTree(selectBody);
        } else
            throw new SelectQueryException("Not a SELECT statement.");
    }

    private List<Map<String, String>> processSelectBodyTree(Select selectBody) {
        if (selectBody instanceof PlainSelect plainSelect) {
            List<Map<String, String>> rows = new ArrayList<>();
            Map<List<String>, List<Map<String, String>>> rowsAfterGroupBy;

            rows = handleJoin(plainSelect);
            rows = handleWhere(plainSelect, rows);
            if (plainSelect.getGroupBy() != null) {
                rows = handleGroupBy(plainSelect, rows);
            } else
                rows = filterRows(selectBody, rows);

            return rows;
        } else throw new SelectQueryException("Something went wrong!");
    }

    private List<Map<String, String>> handleGroupBy(PlainSelect plainSelect, List<Map<String, String>> rows) {
        GroupByElement groupByExpression = plainSelect.getGroupBy();
        Map<List<String>, List<Map<String, String>>> rowsAfterGroupBy;

        if (groupByExpression != null) {
            List<String> groupByList = groupByExpression.getGroupByExpressionList().stream().map(Object::toString).toList();
            rowsAfterGroupBy = groupByService.doGroupBy(groupByList, rows);
            rows = groupByService.filterRowsGroupBy(plainSelect, rowsAfterGroupBy, groupByList);

            Expression havingExpression = plainSelect.getHaving();
            if (havingExpression != null) {
                rows = groupByService.handleHaving(havingExpression, rows);
            }
        }

        return rows;
    }

    private List<Map<String, String>> filterRows(Select selectBody, List<Map<String, String>> rows) {
        PlainSelect plainSelect = (PlainSelect) selectBody;
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        List<String> selectedItems = selectItems
                .stream()
                .map(SelectItem::toString)
                .toList();
        boolean distinctFlag = selectBody.toString().toUpperCase().contains("DISTINCT");
        return filterSelectFields(rows, selectedItems, distinctFlag);
    }

    private List<Map<String, String>> handleWhere(PlainSelect plainSelect, List<Map<String, String>> rows) {
        var whereExpression = plainSelect.getWhere();
        if (whereExpression != null) {
            rows = whereClauseService.handleWhereClause(whereExpression, databaseName, rows);
        }

        return rows;
    }

    private List<Map<String, String>> handleJoin(PlainSelect plainSelect) {
        String fromTableName = plainSelect.getFromItem().toString();

        List<Map<String, String>> rows;
        if (plainSelect.getJoins() != null) {
            rows = joinExecutor.executeJoin(plainSelect.getJoins(), databaseName);
        } else {
            List<SelectAllResponse> allRows = mongoService.selectAll(databaseName, fromTableName);
            Table table = jsonUtil.getTable(fromTableName, databaseName);
            rows = allRows
                    .stream()
                    .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table)))
                    .toList();
        }

        return rows;
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
     * @param selectItems [name, age]
     * @return
     */
    private List<Map<String, String>> filterSelectFields(List<Map<String, String>> rows, List<String> selectItems, boolean distinctFlag) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> row : rows) {
            Map<String, String> resultJson = new HashMap<>();
            for (String key : row.keySet()) {
                if (selectItems.contains(key) || selectItems.contains("*")) {
                    resultJson.put(key, row.get(key)); // {"name": "raul", "age": 21}}
                }

            }
            if (distinctFlag) {
                var theSame = result.stream()
                        .filter(json -> areMapsEqual(json, resultJson))
                        .toList();
                if (theSame.isEmpty())
                    result.add(resultJson);
            } else
                result.add(resultJson);
        }

        return result;
    }

    public static boolean areMapsEqual(Map<String, String> map1, Map<String, String> map2) {
        if (map1 == null && map2 == null) {
            return true;
        }

        if (map1 == null || map2 == null || map1.size() != map2.size()) {
            return false;
        }

        for (Map.Entry<String, String> entry : map1.entrySet()) {
            String key = entry.getKey();
            Object value1 = entry.getValue();
            Object value2 = map2.get(key);

            if (!Objects.equals(value1, value2)) {
                return false;
            }
        }

        return true;
    }
}
