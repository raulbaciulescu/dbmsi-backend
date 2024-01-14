package com.university.dbmsibackend.util;

import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.service.MongoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class JoinUtil {
    @Autowired
    private MongoService mongoService;

    public List<Map<String, String>> mergeMapWithPrimaryKeys(Map<String, String> map1, List<String> primaryKeys, String tableName1, Table table2, String databaseName) {
        List<Map<String, String>> result = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            Map<String, String> commmonMap = new HashMap<>();
            Map<String, String> map2 = mongoService.getByPrimaryKey(table2, primaryKey, databaseName);
            for (String key : map1.keySet()) {
                commmonMap.put(tableName1 + "." + key, map1.get(key));
            }
            for (String key : map2.keySet()) {
                commmonMap.put(table2.getName() + "." + key, map2.get(key));
            }
            result.add(commmonMap);
        }

        return result;
    }

    public Map<String, String> mergeMaps(Map<String, String> map1, Map<String, String> map2, String tableName1, String tableName2) {
        Map<String, String> commmonMap = new HashMap<>();
        for (String key : map1.keySet()) {
            commmonMap.put(tableName1 + "." + key, map1.get(key));
        }
        for (String key : map2.keySet()) {
            commmonMap.put(tableName2 + "." + key, map2.get(key));
        }

        return commmonMap;
    }
}
