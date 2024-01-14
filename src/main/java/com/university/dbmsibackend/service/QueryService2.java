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
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class QueryService2 {
    private final MongoClient mongoClient;
    private final MongoService mongoService;
    private final WhereClauseService2 whereClauseService;
    private final JoinServiceTemp joinServiceTemp;
    private final IndexNestedLoopJoinService indexNestedLoopJoinService;
    private final JsonUtil jsonUtil;
    private String databaseName;

    public QueryService2(JsonUtil jsonUtil,
                         MongoClient mongoClient,
                         MongoService mongoService,
                         WhereClauseService2 whereClauseService,
                         IndexNestedLoopJoinService indexNestedLoopJoinService,
                         JoinServiceTemp joinServiceTemp) {
        this.jsonUtil = jsonUtil;
        this.mongoClient = mongoClient;
        this.mongoService = mongoService;
        this.whereClauseService = whereClauseService;
        this.joinServiceTemp = joinServiceTemp;
        this.indexNestedLoopJoinService = indexNestedLoopJoinService;
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
        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            List<Map<String, String>> rows = new ArrayList<>();

            rows = handleJoin(plainSelect);
            rows = handleWhere(plainSelect, rows);
            rows = filterRows(selectBody, rows);

            return rows;
        } else throw new SelectQueryException("Something went wrong!");
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
            rows = indexNestedLoopJoinService.doJoin(plainSelect.getJoins(), databaseName);
        } else {
            List<SelectAllResponse> resultOfJoins2 = mongoService.selectAll(databaseName, fromTableName);
            Table table = jsonUtil.getTable(fromTableName, databaseName);
            rows = resultOfJoins2
                    .stream()
                    .map(s -> Mapper.dictionaryToMap(TableMapper.mapKeyValueToTableRow(s.key(), s.value(), table)))
                    .toList();
        }

        return rows;
    }

    private List<SelectAllResponse> getRowsByPrimaryKeys(Map<String, List<String>> tableNamePrimaryKeysMap) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        List<SelectAllResponse> response = new ArrayList<>();

        for (String tableName : tableNamePrimaryKeysMap.keySet()) {
            MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
            FindIterable<Document> documents = collection.find(Filters.in("_id", tableNamePrimaryKeysMap.get(tableName)));
            for (Document document : documents) {
                response.add(new SelectAllResponse(document.get("_id").toString(), document.get("value").toString()));
            }
        }

        return response;
    }

    private List<SelectAllResponse> selectAll(Map<String, List<String>> tableNamePrimaryKeysMap) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        List<SelectAllResponse> response = new ArrayList<>();

        for (String tableName : tableNamePrimaryKeysMap.keySet()) {
            MongoCollection<Document> collection = database.getCollection(tableName + ".kv");
            FindIterable<Document> documents = collection.find(Filters.in("_id", tableNamePrimaryKeysMap.get(tableName)));
            for (Document document : documents) {
                response.add(new SelectAllResponse(document.get("_id").toString(), document.get("value").toString()));
            }
        }

        return response;
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
            return true; // Ambele hărți sunt nule, le considerăm egale
        }

        if (map1 == null || map2 == null || map1.size() != map2.size()) {
            return false; // Dacă una este nulă sau au dimensiuni diferite, nu sunt egale
        }

        for (Map.Entry<String, String> entry : map1.entrySet()) {
            String key = entry.getKey();
            Object value1 = entry.getValue();
            Object value2 = map2.get(key);

            if (!Objects.equals(value1, value2)) {
                return false; // Valorile asociate aceleiași chei sunt diferite
            }
        }

        return true; // Hărțile sunt egale
    }
}
