package com.university.dbmsibackend;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

public class CustomSqlParser2 {

    public static void main(String[] args) {
        String sqlQuery = "SELECT users.id, users.name, orders.order_date, payments.amount " +
                "FROM users" +
                "909 INNER JOIN orders ON users.id = orders.user_id " +
                "LEFT JOIN payments ON users.id = payments.user_id " +
                "WHERE users.age > 25 AND orders.total_amount > 1000 " +
                "ORDER BY users.name";

        try {
            // Parse the SQL query
            Statement statement = CCJSqlParserUtil.parse(sqlQuery);

            // Print the traversable tree
            printSqlTree(statement, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printSqlTree(Statement statement, int indentLevel) {
        if (statement instanceof Select) {
            Select select = (Select) statement;
            Select selectBody = select.getSelectBody();
            printNode("Select", indentLevel);
            printSelectBodyTree(selectBody, indentLevel + 1);
        } else {
            System.out.println("Not a SELECT statement.");
        }
    }

    private static void printSelectBodyTree(Select selectBody, int indentLevel) {
        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;

            // Print FROM
            printNode("From", indentLevel);
            printNode("Table: " + plainSelect.getFromItem(), indentLevel + 1);

            // Print JOINs
            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    printNode("Join", indentLevel);
//                    printNode("JoinType: " + join.(), indentLevel + 1);
                    printNode("RightItem", indentLevel + 1);
                    printNode("Table: " + join.getRightItem(), indentLevel + 2);
                    printNode("OnExpression", indentLevel + 1);
                    printExpressionTree(join.getOnExpression(), indentLevel + 2);
                }
            }

            // Print WHERE
            if (plainSelect.getWhere() != null) {
                printNode("Where", indentLevel);
                printExpressionTree(plainSelect.getWhere(), indentLevel + 1);
            }

            // Print ORDER BY
            if (plainSelect.getOrderByElements() != null) {
                printNode("OrderBy", indentLevel);
                for (OrderByElement orderByElement : plainSelect.getOrderByElements()) {
                    printNode("OrderByElement: " + orderByElement.toString(), indentLevel + 1);
                }
            }
        }
    }

    private static void printExpressionTree(Expression expression, int indentLevel) {
        // Implement this method to print the tree structure of an expression
        printNode(expression.toString(), indentLevel);
    }

    private static void printNode(String node, int indentLevel) {
        StringBuilder indentation = new StringBuilder();
        for (int i = 0; i < indentLevel; i++) {
            indentation.append("  ");
        }
        System.out.println(indentation.toString() + "- " + node);
    }
}