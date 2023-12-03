package com.university.dbmsibackend.util;

import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Table;
import org.bson.Document;

import java.util.Dictionary;
import java.util.Hashtable;

public class TableMapper {
    public static Dictionary<String, String> mapMongoEntryToTableRow(Document mongoEntry, Table table) {
        String[] primaryKeys = mongoEntry.get("_id").toString().split("#", -1);
        String[] values = mongoEntry.get("value").toString().split("#", -1);
        return mapToTableRow(table, primaryKeys, values);
    }

    public static Dictionary<String, String> mapKeyValueToTableRow(String key, String value, Table table) {
        String[] primaryKeys = key.split("#", -1);
        String[] values = value.split("#", -1);
        return mapToTableRow(table, primaryKeys, values);
    }

    private static Dictionary<String, String> mapToTableRow(Table table, String[] primaryKeys, String[] values) {
        Dictionary<String, String> resultRow = new Hashtable<>();

        int primaryKeysIndex = 0, valuesIndex = 0;
        for (Attribute attribute : table.getAttributes()) {
            if (table.getPrimaryKeys().contains(attribute.getName())) {
                resultRow.put(attribute.getName(), primaryKeys[primaryKeysIndex]);
                primaryKeysIndex++;
            } else {
                resultRow.put(attribute.getName(), values[valuesIndex]);
                valuesIndex++;
            }
        }
        return resultRow;
    }
}
