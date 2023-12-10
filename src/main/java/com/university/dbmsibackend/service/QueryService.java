package com.university.dbmsibackend.service;

import com.mongodb.client.MongoClient;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.QueryRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.exception.SelectQueryException;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.Mapper;
import com.university.dbmsibackend.util.TableMapper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.springframework.stereotype.Service;


import java.util.*;

@Service
public class QueryService {
    private final MongoClient mongoClient;
    private final MongoService mongoService;
    private final WhereClauseService whereClauseService;
    private final JoinService joinService;
    private final JsonUtil jsonUtil;
    private String databaseName;

    public QueryService(JsonUtil jsonUtil,
                        MongoClient mongoClient,
                        MongoService mongoService,
                        WhereClauseService whereClauseService,
                        JoinService joinService) {
        this.jsonUtil = jsonUtil;
        this.mongoClient = mongoClient;
        this.mongoService = mongoService;
        this.whereClauseService = whereClauseService;
        this.joinService = joinService;
        this.databaseName = "";
    }

    public List<Map<String, Object>> executeQuery(QueryRequest request) {
        String query = request.query();
        databaseName = request.databaseName();
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            return processSqlTree(statement);
        } catch (JSQLParserException e) {
            throw new SelectQueryException(e.getMessage());
        }
    }

    private List<Map<String, Object>> processSqlTree(Statement statement) {
        if (statement instanceof Select select) {
            Select selectBody = select.getSelectBody();
            return processSelectBodyTree(selectBody);
        } else
            throw new SelectQueryException("Not a SELECT statement.");
    }

    private List<Map<String, Object>> processSelectBodyTree(Select selectBody) {
        String fromTableName;

        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            fromTableName = plainSelect.getFromItem().toString();
            Table table = jsonUtil.getTable(fromTableName, databaseName);

//            creare lista totala si parsare randuri
            List<SelectAllResponse> rows = mongoService.selectAll(databaseName, fromTableName);
            List<Map<String, Object>> result = new ArrayList<>();
            for (SelectAllResponse row : rows) {
                Map<String, Object> jsonRow = mapRow(row.key(), row.value(), table);
                result.add(jsonRow);
            }

            var whereExpression = plainSelect.getWhere();
            if (whereExpression != null) {
                result = whereClauseService.handleWhereClause(result, whereExpression, databaseName);
            }

            // Print JOINs
            List<Map<String, String>> resultOfJoins = new ArrayList<>();
            if (plainSelect.getJoins() != null) {
                resultOfJoins = joinService.handleJoin(plainSelect.getJoins(), databaseName);
            }

            List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
            List<String> selectedItems = selectItems
                    .stream()
                    .map(SelectItem::toString)
                    .toList();
            result = filterSelectFields(result, selectedItems);

            return result;
        } else throw new SelectQueryException("DA");
    }

    private static Object convertStringToCorrectType(String type, String value) {
        return switch (type) {
            case "varchar" -> value;
            case "integer" -> Integer.parseInt(value);
            case "float" -> Float.parseFloat(value);
            case "bool" -> Boolean.parseBoolean(value);
            default -> null;
        };
    }

    private Map<String, Object> mapRow(String key, String value, Table table) {
        String[] primaryKeys = key.split("#", -1);
        String[] values = value.split("#", -1);

        Map<String, Object> result = new HashMap<>();

        int primaryKeysIndex = 0, valuesIndex = 0;
        for (Attribute attribute : table.getAttributes()) {
            if (table.getPrimaryKeys().contains(attribute.getName())) {
                result.put(attribute.getName(),
                        convertStringToCorrectType(attribute.getType(), primaryKeys[primaryKeysIndex]));
                primaryKeysIndex++;
            } else {
                result.put(attribute.getName(),
                        convertStringToCorrectType(attribute.getType(), values[valuesIndex]));
                valuesIndex++;
            }
        }

        return result;
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
     * @param selectItems   [name, age]
     * @return
     */
    private List<Map<String, Object>> filterSelectFields(List<Map<String, Object>> rows, List<String> selectItems) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> resultJson = new HashMap<>();
            for (String key : row.keySet()) {
                if (selectItems.contains(key) || selectItems.contains("*"))
                    resultJson.put(key, row.get(key)); //{"name": "raul", "age": 21}
            }
            result.add(resultJson);
        }

        return result;
    }
}
