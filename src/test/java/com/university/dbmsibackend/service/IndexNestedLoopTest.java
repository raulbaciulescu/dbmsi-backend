package com.university.dbmsibackend.service;

import com.university.dbmsibackend.domain.Operation;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Map;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource("classpath:application-test.properties")
class JoinTest {
    @Autowired
    private IndexNestedLoopJoinService indexNestedLoopJoinService;
    @Autowired
    private SortMergeJoinService sortMergeJoinService;
    @Autowired
    private WhereClauseService2 whereClauseService;

    @Test
    void doJoinTest() {
        String tableName1 = "student";
        String tableName2 = "group";
        String column1 = "firstName";
        String column2 = "id";
        String databaseName = "university4";
        Operation predicate = Operation.EQUALS;

        var result = indexNestedLoopJoinService.doJoin(
                tableName1,
                tableName2,
                column1,
                column2,
                databaseName,
                predicate
        );
        System.out.println(result);
        var result2 = sortMergeJoinService.doJoin(
                tableName1,
                tableName2,
                column1,
                column2,
                databaseName,
                predicate
        );
        System.out.println(result2);

        String columnName = "groupId";
        String sqlQuery = "SELECT groupId FROM student WHERE student.firstName > 2 and group.id = 8";
        try {
            // Parse the SQL query
            Statement statement = CCJSqlParserUtil.parse(sqlQuery);
            printSqlTree(statement, databaseName, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void printSqlTree(Statement statement, String databaseName, List<Map<String, String>> result) {
        if (statement instanceof Select) {
            Select select = (Select) statement;
            Select selectBody = select.getSelectBody();
            printSelectBodyTree(selectBody, databaseName, result);
        } else {
            System.out.println("Not a SELECT statement.");
        }
    }

    private void printSelectBodyTree(Select selectBody, String databaseName, List<Map<String, String>> rows) {
        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;

            // Print WHERE
            if (plainSelect.getWhere() != null) {
                var result = whereClauseService.handleWhereClause(
                        plainSelect.getWhere(),
                        databaseName,
                        rows
                );
                System.out.println("Result: " + result);
            }
        }
    }
}