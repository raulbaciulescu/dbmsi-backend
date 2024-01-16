package com.university.dbmsibackend.service;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class GroupByService {
    public Map<List<String>, List<Map<String, String>>> doGroupBy(List<String> groupByList, List<Map<String, String>> rows) {
        return rows.stream()
                .collect(Collectors.groupingBy(map ->
                        groupByList.stream()
                                .map(map::get)
                                .collect(Collectors.toList())
                ));
    }

    List<Map<String, String>> filterRowsGroupBy(PlainSelect plainSelect, Map<List<String>, List<Map<String, String>>> rowsAfterGroupBy, List<String> groupByList) {
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        List<Map<String, String>> rows = new ArrayList<>();
        List<String> operations = new ArrayList<>();

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
            }
            if (matcherMin.find()) {
                operations.add("min^" + matcherMin.group(1));
            }
            if (matcherMax.find()) {
                operations.add("max^" + matcherMax.group(1));
            }
            if (matcherSum.find()) {
                operations.add("sum^" + matcherSum.group(1));
            }
        }
        handleMin(rows, rowsAfterGroupBy, groupByList, operations);

        return rows;
    }

    private void handleMin(List<Map<String, String>> rows, Map<List<String>,
            List<Map<String, String>>> rowsAfterGroupBy, List<String> groupByList, List<String> operationList) {
        rowsAfterGroupBy.forEach((keyList, value) -> {
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < groupByList.size(); i++) {
                map.put(groupByList.get(i), keyList.get(i));
            }

            for (String operation : operationList) {
                if (operation.matches("count")) {
                    map.put("count", String.valueOf(value.size()));
                }
                if (operation.matches("min.*")) {
                    String minKey = operation.split("\\^")[1];
                    int minValue = 1_000_000;
                    String minString = "zzzzzzzzzzzzz";
                    for (Map<String, String> map2 : value) {
                        if (canConvertToInt(map2.get(minKey))) {
                            int intValue = Integer.parseInt(map2.get(minKey));
                            minValue = Math.min(minValue, intValue);
                        } else if (map2.get(minKey).compareTo(minString) < 0) {
                            minString = map2.get(minKey);
                        }
                    }
                    if (minValue != 1_000_000)
                        map.put("min " + minKey, String.valueOf(minValue));
                    else
                        map.put("min " + minKey, minString);
                }
                if (operation.matches("max.*")) {
                    String maxKey = operation.split("\\^")[1];
                    int maxValue = -1;
                    String maxString = "aaaaaaaaaaaaaa";
                    for (Map<String, String> map2 : value) {
                        if (canConvertToInt(map2.get(maxKey))) {
                            int intValue = Integer.parseInt(map2.get(maxKey));
                            maxValue = Math.max(maxValue, intValue);
                        } else if (map2.get(maxKey).compareTo(maxString) > 0) {
                            maxString = map2.get(maxKey);
                        }
                    }
                    if (maxValue != -1)
                        map.put("max " + maxKey, String.valueOf(maxValue));
                    else
                        map.put("max " + maxKey, maxString);
                }
                if (operation.matches("sum.*")) {
                    String sumKey = operation.split("\\^")[1];
                    int sum = 0;
                    for (Map<String, String> map2 : value) {
                        if (canConvertToInt(map2.get(sumKey))) {
                            int intValue = Integer.parseInt(map2.get(sumKey));
                            sum += intValue;
                        }
                    }
                    map.put("sum " + sumKey, String.valueOf(sum));
                }
            }

            rows.add(map);
        });

        System.out.println(rows);
    }

    public static boolean canConvertToInt(String str) {
        try {
            // Attempt to convert the string to an int
            Integer.parseInt(str);
            return true; // Conversion successful
        } catch (NumberFormatException e) {
            return false; // Conversion failed
        }
    }
}
