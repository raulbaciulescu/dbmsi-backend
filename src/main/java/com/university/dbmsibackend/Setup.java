//package com.university.dbmsibackend;
//
//import com.university.dbmsibackend.domain.Attribute;
//import com.university.dbmsibackend.domain.ForeignKey;
//import com.university.dbmsibackend.dto.*;
//import com.university.dbmsibackend.service.DatabaseService;
//import com.university.dbmsibackend.service.ForeignKeyService;
//import com.university.dbmsibackend.service.IndexService;
//import com.university.dbmsibackend.service.TableService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//
//import java.util.List;
//import java.util.Random;
//
//@SpringBootApplication
//public class Setup implements CommandLineRunner {
//
//    @Autowired
//    private TableService tableService;
//    @Autowired
//    private DatabaseService databaseService;
//    @Autowired
//    private ForeignKeyService foreignKeyService;
//    @Autowired
//    private IndexService indexService;
//    private final String databaseName = "university4";
//
//    public static void main(String[] args) {
//        SpringApplication.run(Setup.class, args);
//    }
//
//    @Override
//    public void run(String... args) throws Exception {
////        createTables();
////        createIndex();
////        insertInTables();
//    }
//
//    private void createIndex() {
//        String tableName1 = "student";
//
//        indexService.createIndex(new CreateIndexRequest(
//                "fistNameIndex",
//                false,
//                "BTree",
//                tableName1,
//                databaseName,
//                List.of(new Attribute("firstName", "varchar", null, false))
//        ));
//    }
//
//    private void insertInTables() {
//        Random random = new Random();
//        String tableName1 = "student";
//        String tableName2 = "group";
//
//
//        for (int i = 1; i <= 10; i++) {
//            String value = "group" + i + "#proff" + i;
//            tableService.insertRow(new InsertRequest(
//                    Integer.toString(i),
//                    value,
//                    tableName2,
//                    databaseName
//            ));
//        }
//        int x = 0;
//        for (int i = 1; i <= 50; i++) {
//            String groupIdRandom = String.valueOf(random.nextInt(9) + 1);
//            String firstName = String.valueOf(random.nextInt(20) + 1);
//            String value = firstName + "#lastName" + i + "#" + i + "#" + groupIdRandom;
//            tableService.insertRow(new InsertRequest(
//                    Integer.toString(i),
//                    value,
//                    tableName1,
//                    databaseName
//            ));
//        }
//        System.out.println(x);
//    }
//
//    private void createTables() {
//        databaseService.createDatabase(new CreateDatabaseRequest(databaseName));
//
//        String tableName2 = "group";
//        List<Attribute> table2Attributes = List.of(
//                new Attribute("id", "integer", null, false),
//                new Attribute("groupName", "varchar", null, false),
//                new Attribute("professor", "varchar", null, false)
//        );
//        List<String> primaryKeysTable2 = List.of("id");
//
//
//        tableService.createTable(new CreateTableRequest(databaseName, tableName2, table2Attributes, primaryKeysTable2, List.of()));
//
//
//        String tableName1 = "student";
//        List<Attribute> table1Attributes = List.of(
//                new Attribute("id", "integer", null, false),
//                new Attribute("firstName", "varchar", null, false),
//                new Attribute("lastName", "varchar", null, false),
//                new Attribute("age", "integer", null, false),
//                new Attribute("groupId", "integer", null, false)
//        );
//        List<String> primaryKeysTable1 = List.of("id");
//
//        tableService.createTable(new CreateTableRequest(databaseName, tableName1, table1Attributes, primaryKeysTable1, List.of()));
//        foreignKeyService.createForeignKey(new CreateForeignKeyRequest(
//                "fk_groupId",
//                databaseName,
//                tableName1,
//                List.of(new Attribute("groupId", "integer", null, false)),
//                "group",
//                List.of(new Attribute("id", "integer", null, false))
//        ));
//    }
//}