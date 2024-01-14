package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.IndexFileValue;
import com.university.dbmsibackend.domain.Operation;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.util.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class SortMergeJoin {
    @Autowired
    private JsonUtil jsonUtil;
    @Autowired
    private MongoService mongoService;

    public List<Map<String, String>> doJoin(String tableName1, String tableName2, String column1, String column2, String databaseName, Operation predicate) {
//        int mark, r, s;
//        do {
//            if (mark == -1) {
//                while (r < s) {
//                    //advance r
//                }
//                while (r > s) {
//                    //advance s
//                }
//                //mark = s
//            }
//            if (r == s) {
//                //result.add(merge(r, s))
//                //advance s
//            } else {
//                // reset s to mark
//                //advance r
//                //mark = -1;
//            }
//        }
        return null;
    }
}
