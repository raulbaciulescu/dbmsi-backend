package com.university.dbmsibackend.util;

import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

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
}
