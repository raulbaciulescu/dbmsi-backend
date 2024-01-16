package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Operation;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.service.api.JoinService;
import com.university.dbmsibackend.util.JoinUtil;
import com.university.dbmsibackend.util.JsonUtil;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@AllArgsConstructor
public class SortMergeJoinService implements JoinService {
    private JsonUtil jsonUtil;
    private MongoService mongoService;
    private JoinUtil joinUtil;

    @Override
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
        } else if (!hasIndex1 && !hasIndex2) { // nestedLoop
            return sortMergeJoinWithoutIndex(tableName1, tableName2, column1, column2, databaseName, predicate);
        }

        System.out.println("sort join with index");
        List<Map<String, String>> table1RowsJsons = mongoService.getTableJsonList(tableName1, databaseName);
        List<IndexFileValue> table2IndexValues = mongoService.getIndexValues(tableName2, column2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Table table2 = jsonUtil.getTable(tableName2, databaseName);

        String finalColumn1 = column1;
        Comparator<Map<String, String>> mapComparator = (m1, m2) -> {
            Integer i1 = Integer.parseInt(m1.get(finalColumn1));
            Integer i2 = Integer.parseInt(m2.get(finalColumn1));
            return i1.compareTo(i2);
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

    @Override
    public List<Map<String, String>> secondJoin(List<Map<String, String>> rows, String tableName1, String tableName2,
                                                String column1, String column2, String databaseName, Operation predicate) {
        boolean hasIndex2 = jsonUtil.hasIndex(tableName2, column2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Table table2 = jsonUtil.getTable(tableName2, databaseName);

        if (hasIndex2) {
            System.out.println("Second sort join hasIndex");
            List<IndexFileValue> table2IndexValues = mongoService.getIndexValues(tableName2, column2, databaseName);

            String finalColumn1 = tableName1 + "." + column1;
            Comparator<Map<String, String>> mapComparator = (m1, m2) -> {
                Integer i1 = Integer.parseInt(m1.get(finalColumn1));
                Integer i2 = Integer.parseInt(m2.get(finalColumn1));
                return i1.compareTo(i2);
            };
            rows.sort(mapComparator);
            table2IndexValues = table2IndexValues.stream()
                    .sorted(Comparator.comparing(indexFileValue -> Integer.parseInt(indexFileValue.value())))
                    .toList();

            int mark = -1, r = 0, s = 0;
            do {
                if (mark == -1) {
                    while (r < rows.size() && compare(predicate, rows.get(r).get(tableName1 + "." + column1), table2IndexValues.get(s).value()) < 0)
                        r++;
                    while (s < table2IndexValues.size() && compare(predicate, rows.get(r).get(tableName1 + "." + column1), table2IndexValues.get(s).value()) > 0)
                        s++;
                    mark = s;
                }
                if (compare(predicate, rows.get(r).get(tableName1 + "." + column1), table2IndexValues.get(s).value()) == 0) {
                    result.addAll(joinUtil.mergeMapWithPrimaryKeys(rows.get(r), table2IndexValues.get(s).primaryKeys(), tableName1, table2, databaseName));
                    s++;
                } else {
                    s = mark;
                    r++;
                    mark = -1;
                }
            } while (r < rows.size() && s < table2IndexValues.size());
        } else {
            System.out.println("Second sort join hasn't Index");
            List<Map<String, String>> table2RowsJsons = mongoService.getTableJsonList(tableName2, databaseName);
            Comparator<Map<String, String>> mapComparator2 = (m1, m2) -> {
                Integer i1 = Integer.parseInt(m1.get(column2));
                Integer i2 = Integer.parseInt(m2.get(column2));
                return i1.compareTo(i2);
            };
            String finalColumn1 = tableName1 + "." + column1;
            Comparator<Map<String, String>> mapComparator = (m1, m2) -> {
                Integer i1 = Integer.parseInt(m1.get(finalColumn1));
                Integer i2 = Integer.parseInt(m2.get(finalColumn1));
                return i1.compareTo(i2);
            };
            rows.sort(mapComparator);
            table2RowsJsons.sort(mapComparator2);
            int mark = -1, r = 0, s = 0;
            do {
                if (mark == -1) {
                    while (r < rows.size() && compare(predicate, rows.get(r).get(tableName1 + "." + column1), table2RowsJsons.get(s).get(column2)) < 0)
                        r++;
                    while (s < table2RowsJsons.size() && compare(predicate, rows.get(r).get(tableName1 + "." + column1), table2RowsJsons.get(s).get(column2)) > 0)
                        s++;
                    mark = s;
                }
                if (r < rows.size() && s < table2RowsJsons.size() &&
                        compare(predicate, rows.get(r).get(tableName1 + "." + column1), table2RowsJsons.get(s).get(column2)) == 0) {
                    result.add(joinUtil.mergeMaps(rows.get(r), table2RowsJsons.get(s), tableName1, tableName2));
                    s++;
                } else {
                    s = mark;
                    r++;
                    mark = -1;
                }
            } while (r < rows.size() && s < table2RowsJsons.size());
        }

        return result;
    }

    private int compare(Operation predicate, String value, String s) {
        return Integer.compare(Integer.parseInt(value), Integer.parseInt(s));
    }

    private List<Map<String, String>> sortMergeJoinWithoutIndex(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
        System.out.println("sort Join without index");
        List<Map<String, String>> table1RowsJsons = mongoService.getTableJsonList(tableName1, databaseName);
        List<Map<String, String>> table2RowsJsons = mongoService.getTableJsonList(tableName2, databaseName);
        List<Map<String, String>> result = new ArrayList<>();
        Comparator<Map<String, String>> mapComparator = (m1, m2) -> {
            Integer i1 = Integer.parseInt(m1.get(column1));
            Integer i2 = Integer.parseInt(m2.get(column1));
            return i1.compareTo(i2);
        };
        Comparator<Map<String, String>> mapComparator2 = (m1, m2) -> {
            Integer i1 = Integer.parseInt(m1.get(column2));
            Integer i2 = Integer.parseInt(m2.get(column2));
            return i1.compareTo(i2);
        };

        table1RowsJsons.sort(mapComparator);
        table2RowsJsons.sort(mapComparator2);
        int mark = -1, r = 0, s = 0;
        do {
            if (mark == -1) {
                while (r < table1RowsJsons.size() && compare(predicate, table1RowsJsons.get(r).get(column1), table2RowsJsons.get(s).get(column2)) < 0)
                    r++;
                while (s < table2RowsJsons.size() && compare(predicate, table1RowsJsons.get(r).get(column1), table2RowsJsons.get(s).get(column2)) > 0)
                    s++;
                mark = s;
            }
            if (r < table1RowsJsons.size() && s < table2RowsJsons.size() &&
                    compare(predicate, table1RowsJsons.get(r).get(column1), table2RowsJsons.get(s).get(column2)) == 0) {
                result.add(joinUtil.mergeMaps(table1RowsJsons.get(r), table2RowsJsons.get(s), tableName1, tableName2));
                s++;
            } else {
                s = mark;
                r++;
                mark = -1;
            }
        } while (r < table1RowsJsons.size() && s < table2RowsJsons.size());

        return result;
    }
}
