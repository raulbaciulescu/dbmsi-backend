package com.university.dbmsibackend.service;

import com.mongodb.client.MongoClient;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.dto.QueryRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.util.JsonUtil;
import com.university.dbmsibackend.util.TableMapper;
import lombok.AllArgsConstructor;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@AllArgsConstructor
public class QueryService {
    private MongoClient mongoClient;
    private MongoService mongoService;
    private JsonUtil jsonUtil;

    public void executeQuery(QueryRequest request) {
        System.out.println(request.query());
        String query = request.query();
        try {
            // Parse the SQL query
            Statement statement = CCJSqlParserUtil.parse(query);

            // Print the traversable tree
            printSqlTree(statement);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void printSqlTree(Statement statement) throws Exception {
        if (statement instanceof Select) {
            Select select = (Select) statement;
            Select selectBody = select.getSelectBody();
            System.out.println(printSelectBodyTree(selectBody));
        } else {
            System.out.println("Not a SELECT statement.");
        }
    }

    private List<Map<String, String>> printSelectBodyTree(Select selectBody) throws Exception {
        String fromTableName;

        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;

            fromTableName = plainSelect.getFromItem().toString();
            List<SelectItem<?>> selectItem = plainSelect.getSelectItems();
            List<Map<String, String>> result = selectSimpleQuery(fromTableName, selectItem);

//            // Print JOINs
//            if (plainSelect.getJoins() != null) {
//                for (Join join : plainSelect.getJoins()) {
//                    printNode("Join", indentLevel);
////                    printNode("JoinType: " + join.(), indentLevel + 1);
//                    printNode("RightItem", indentLevel + 1);
//                    printNode("Table: " + join.getRightItem(), indentLevel + 2);
//                    printNode("OnExpression", indentLevel + 1);
//                    printExpressionTree(join.getOnExpression(), indentLevel + 2);
//                }
//            }

            // Print WHERE
//            if (plainSelect.getWhere() != null) {
//                printExpressionTree(plainSelect.getWhere(),);
//            }

            return result;
        } else throw new Exception("DA");
    }

    /**
     * {"id": 1, "name": "raul", "age": 21, "dadada": "DADADA"}
     * @param fromTableName
     * @param selectItems [name, age]
     * @return
     */
    private List<Map<String, String>> selectSimpleQuery(String fromTableName, List<SelectItem<?>> selectItems) {
        List<SelectAllResponse> rows = mongoService.selectAll("university", fromTableName);
        Table table = jsonUtil.getTable(fromTableName, "university");
        List<Map<String, String>> result = new ArrayList<>();
        for (SelectAllResponse row: rows) {
            Map<String, String> jsonRow = dictionaryToMap(TableMapper.mapKeyValueToTableRow(row.key(), row.value(), table));
            Map<String, String> resultJson = new HashMap<>();
            for (String key : jsonRow.keySet()) {
                if (selectItems.contains(key))
                    resultJson.put(key, jsonRow.get(key)); //{"name": "raul", "age": 21}
            }
            result.add(resultJson);
        }

        return result;
    }

    private static <K, V> Map<K, V> dictionaryToMap(Dictionary<K, V> dictionary) {
        Map<K, V> map = new HashMap<>();

        // Iterate over the keys in the Dictionary and add them to the Map
        Enumeration<K> keys = dictionary.keys();
        while (keys.hasMoreElements()) {
            K key = keys.nextElement();
            V value = dictionary.get(key);
            map.put(key, value);
        }

        return map;
    }
}
