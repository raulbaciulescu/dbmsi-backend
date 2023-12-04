package com.university.dbmsibackend;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.select.*;

public class CustomSqlParser {

    public static void main(String[] args) throws JSQLParserException {
        // Define an Expression Visitor reacting on any Expression
// Overwrite the visit() methods for each Expression Class
        ExpressionVisitorAdapter expressionVisitorAdapter = new ExpressionVisitorAdapter() {
            public void visit(EqualsTo equalsTo) {
                System.out.println("equals");
                equalsTo.getLeftExpression().accept(this);
                equalsTo.getRightExpression().accept(this);
            }

            public void visit(GreaterThanEquals equalsTo) {
                System.out.println("greater");
            }


            public void visit(Column column) {
                System.out.println("Found a Column " + column.getColumnName());
            }
        };

// Define a Select Visitor reacting on a Plain Select invoking the Expression Visitor on the Where Clause
        SelectVisitorAdapter selectVisitorAdapter = new SelectVisitorAdapter() {
            @Override
            public void visit(PlainSelect plainSelect) {
                plainSelect.getWhere().accept(expressionVisitorAdapter);
            }
        };

// Define a Statement Visitor for dispatching the Statements
        StatementVisitorAdapter statementVisitor = new StatementVisitorAdapter() {
            public void visit(Select select) {
                select.getSelectBody().accept(selectVisitorAdapter);
            }
        };

        String sqlStr = "select 1 from dual where a=b and a>=b";
        Statement stmt = CCJSqlParserUtil.parse(sqlStr);

// Invoke the Statement Visitor
        stmt.accept(statementVisitor);
    }
}

