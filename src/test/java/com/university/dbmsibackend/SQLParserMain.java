package com.university.dbmsibackend;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.junit.jupiter.api.Assertions;

public class SQLParserMain {
    public static void main(String[] args) throws JSQLParserException {
        String sqlStr = "select firstName =lastName, da, dasda from da where a>=b and x = y";
//String sqlStr1 = "select 1 from dual where a=b";
//String sqlStr2 = "select * from da where x = y";

        //"select * from da where x = y"
        PlainSelect select = (PlainSelect) CCJSqlParserUtil.parse(sqlStr);
//
        SelectItem selectItem =
                select.getSelectItems().get(0);
        System.out.println(select.getSelectItems().size());
        System.out.println(select.getSelectItems().stream().toList());

        System.out.println(selectItem);
//        Assertions.assertEquals(
//                new LongValue(1)
//                , selectItem.getExpression());
//
        Table table = (Table) select.getFromItem();
        System.out.println(table);

        System.out.println(select.getWhere());
        Expression whereExpression = select.getWhere();
        if (whereExpression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) whereExpression;

            // Extract left and right expressions of the AND condition
            Expression leftExpression = andExpression.getLeftExpression();
            Expression rightExpression = andExpression.getRightExpression();

            System.out.println("Left Expression: " + leftExpression.toString());
            System.out.println("Right Expression: " + rightExpression.toString());
        } else {
            // Handle a single condition
            System.out.println("Single Expression: " + whereExpression.toString());
        }
//        GreaterThanEquals equalsTo = (GreaterThanEquals) select.getWhere();
//        Column a = (Column) equalsTo.getLeftExpression();
//        Column b = (Column) equalsTo.getRightExpression();
//        System.out.println(a);
//        System.out.println(b);
//        Assertions.assertEquals("a", a.getColumnName());
//        Assertions.assertEquals("b", b.getColumnName());
    }
}
