package DB;

import grammar.SQLiteLexer;
import grammar.SQLiteParser;
import grammar.SQLiteParserBaseListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SQLParser {
    private static boolean error =false;
    private static Iterator it;
    private static DBApp DB;
    public static Iterator parse(StringBuffer strbufSQL, DBApp db ) throws DBAppException{
        DB = db;
        CharStream charStream = CharStreams.fromString(strbufSQL.toString());
        SQLiteLexer sqLiteLexer = new SQLiteLexer(charStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(sqLiteLexer);
        SQLiteParser sqLiteParser = new SQLiteParser(commonTokenStream);

        ParseTree tree = sqLiteParser.parse();
        DBApp finalDb = db;
        ParseTreeWalker.DEFAULT.walk(new SQLiteParserBaseListener(){
            @Override
            public void enterSql_stmt(SQLiteParser.Sql_stmtContext ctx) {

            }

            @Override
            public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
                try{
                    String tableName = ctx.select_core(0).table_or_subquery(0).table_name().getText();
                    System.out.println(tableName);
//                    Table t = DBApp.getTable(tableName);

                    SQLiteParser.ExprContext expression = ctx.select_core().get(0).expr().get(0);
                    System.out.println(expression.getText());

                    LinkedList<SQLTerm> sqlTermsll = new LinkedList<>();
                    LinkedList<String> operatorsll = new LinkedList<>();
                    while(expression.AND_()!=null || expression.OR_()!=null){
                        sqlTermsll.addFirst(getSQLTerm(expression.expr().get(1),tableName));
                        operatorsll.addFirst(expression.AND_()==null?"OR":"AND");
                        expression = expression.expr().get(0);
                    }
                    sqlTermsll.addFirst(getSQLTerm(expression,tableName));
                    SQLTerm[] arrSQLTerms =new SQLTerm[sqlTermsll.size()];
                    String[] strarrOperators = new String[operatorsll.size()];

                    for(int i=0;i<sqlTermsll.size();i++){arrSQLTerms[i]=sqlTermsll.get(i);}
                    for(int i=0;i<operatorsll.size();i++){strarrOperators[i]=operatorsll.get(i);}

                    System.out.println(Arrays.toString(arrSQLTerms));
                    System.out.println(Arrays.toString(strarrOperators));
                    db.selectFromTable(arrSQLTerms,strarrOperators);
                }catch (Exception ignored){
                    error = true;
                }
            }

            @Override
            public void enterCreate_index_stmt(SQLiteParser.Create_index_stmtContext ctx) {

            }
            @Override
            public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
                try{

                }catch (Exception ignored){
                    error=true;
                }
            }

            @Override
            public void enterUpdate_stmt(SQLiteParser.Update_stmtContext ctx) {

            }

            @Override
            public void enterInsert_stmt(SQLiteParser.Insert_stmtContext ctx){

            }
            @Override
            public void enterDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {

            }

        },tree);

        if(error){
            throw new DBAppException("Wrong SQl statement");
        }
        return it;
    }

    private static SQLTerm getSQLTerm(SQLiteParser.ExprContext expression,String tableName) throws DBAppException, ParseException {
        String colName = expression.expr().get(0).getText();
        String value = expression.expr().get(1).getText();
        String operand = getOperand(expression);
        SQLTerm sqlTerm = new SQLTerm();
        sqlTerm._strTableName = tableName;
        sqlTerm._strColumnName = colName;
        sqlTerm._strOperator = operand;
        sqlTerm._objValue = getObjectValue(tableName,colName,value);
        return sqlTerm;
    }

    private static Object getObjectValue(String tableName, String colName, String value) throws DBAppException, ParseException {
        Table t = DB.getTable(tableName);
        String type =t.getKeyType(colName);
        Object o = null;
        switch (type) {
            case "java.lang.String" : o = value;break;
            case "java.lang.Integer": o = Integer.parseInt(value);break;
            case "java.lang.Double" :o = Double.parseDouble(value);break;
            case "java.util.Date"   : String date = value ;
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                o = sdf.parse(value);
                break;
        }
        return o;
    }


    private static String getOperand(SQLiteParser.ExprContext expression) {
        String s = "";
        if (expression.ASSIGN() != null) s = "=";
        if (expression.GT_EQ() != null) s = ">=";
        if (expression.GT() != null) s = ">";
        if (expression.LT_EQ() != null) s = "<=";
        if (expression.LT() != null) s = "<";
        if (expression.NOT_EQ1() != null) s = "!=";
        return s;
    }
}




