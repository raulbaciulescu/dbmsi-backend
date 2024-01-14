package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Operation;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.util.JoinUtil;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Service
@AllArgsConstructor
public class SortMergeJoin {
    private JsonUtil jsonUtil;
    private MongoService mongoService;
    private JoinUtil joinUtil;

    public List<Map<String, String>> doJoin(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        boolean hasIndex1 = jsonUtil.hasIndex(tableName1, column1, databaseName);
        boolean hasIndex2 = jsonUtil.hasIndex(tableName2, column2, databaseName);
        if (!hasIndex2 && hasIndex1) {
            String temp = tableName1;
            tableName1 = tableName2;
            tableName2 = temp;

            temp = column1;
            column1 = column2;
            column2 = temp;
        } else if (!hasIndex1) { // nestedLoop
            return sortMergeJoinWithoutIndex(tableName1, tableName2, column1, column2, databaseName, predicate);
        }

        List<Map<String, String>> table1RowsJsons = mongoService.getTableJsonList(tableName1, databaseName);
        List<IndexFileValue> table2IndexValues = mongoService.getIndexValues(tableName2, column2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Table table2 = jsonUtil.getTable(tableName2, databaseName);


        String finalColumn1 = column1;
        Comparator<Map<String, String>> mapComparator = new Comparator<Map<String, String>>() {
            public int compare(Map<String, String> m1, Map<String, String> m2) {
                Integer i1 = Integer.parseInt(m1.get(finalColumn1));
                Integer i2 = Integer.parseInt(m2.get(finalColumn1));
                return i1.compareTo(i2);
            }
        };
        table1RowsJsons.sort(mapComparator);
        table2IndexValues = table2IndexValues.stream()
                .sorted(Comparator.comparing(indexFileValue -> Integer.parseInt(indexFileValue.value())))
                .toList();
        int mark = -1, r = 0, s = 0;
        do {
            if (mark == -1) {
                while (r < table1RowsJsons.size() && compare(predicate, table1RowsJsons.get(r).get(column1), table2IndexValues.get(s).value()) < 0)
                    r++;
                while (s < table2IndexValues.size() && compare(predicate, table1RowsJsons.get(r).get(column1), table2IndexValues.get(s).value()) > 0)
                    s++;
                mark = s;
            }
            if (compare(predicate, table1RowsJsons.get(r).get(column1), table2IndexValues.get(s).value()) == 0) {
                result.addAll(joinUtil.mergeMapWithPrimaryKeys(table1RowsJsons.get(r), table2IndexValues.get(s).primaryKeys(), tableName1, table2, databaseName));
                s++;
            } else {
                s = mark;
                r++;
                mark = -1;
            }
        } while (r < table1RowsJsons.size() && s < table2IndexValues.size());

        return result;
    }

    private int compare(Operation predicate, String value, String s) {
        return Integer.compare(Integer.parseInt(value), Integer.parseInt(s));
    }

    private List<Map<String, String>> sortMergeJoinWithoutIndex(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        return null;
    }
}
