package com.university.dbmsibackend.util;

import com.mongodb.client.FindIterable;
import com.university.dbmsibackend.domain.IndexFileValue;
import org.bson.Document;

import java.util.*;

public class Mapper {
    public static <K1, K2, V> Map<K1, List<Map<K2, V>>> convertMap(Map<K1, List<Dictionary<K2, V>>> inputMap) {
        Map<K1, List<Map<K2, V>>> resultMap = new HashMap<>();

        for (Map.Entry<K1, List<Dictionary<K2, V>>> entry : inputMap.entrySet()) {
            K1 key = entry.getKey();
            List<Dictionary<K2, V>> list = entry.getValue();

            List<Map<K2, V>> convertedList = new ArrayList<>();
            for (Dictionary<K2, V> dictionary : list) {
                convertedList.add(dictionaryToMap(dictionary));
            }

            resultMap.put(key, convertedList);
        }

        return resultMap;
    }

    public static <K, V> Map<K, V> dictionaryToMap(Dictionary<K, V> dictionary) {
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

    public static List<IndexFileValue> mapToIndexFileValue(FindIterable<Document> documents) {
        List<IndexFileValue> indexFileValues = new ArrayList<>();
        for (Document document : documents) {
            String primaryKeys = document.get("primary-key").toString();
            List<String> primaryKeysList = List.of(primaryKeys.split("\\$"));
            indexFileValues.add(new IndexFileValue(document.get("_id").toString(), primaryKeysList));
        }

        return indexFileValues;
    }
}
