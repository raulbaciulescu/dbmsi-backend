package com.university.dbmsibackend.util;

import com.mongodb.client.FindIterable;
import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.dto.SelectAllResponse;
import org.bson.Document;

import java.util.*;

public class Mapper {
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
            List<String> primaryKeysList = Collections.singletonList(String.join("$", primaryKeys));
            indexFileValues.add(new IndexFileValue(document.get("_id").toString(), primaryKeysList));
        }

        return indexFileValues;
    }
}
