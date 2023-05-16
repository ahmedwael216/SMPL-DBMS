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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                try{
                    String tableName = ctx.table_name().getText();
//                    System.out.println("table Name: "+tableName);
                    String[] columnNames = new String[ctx.indexed_column().size()];
                    for(int i=0;i<ctx.indexed_column().size();i++){
                        columnNames[i] = ctx.indexed_column().get(i).getText();
                    }
//                    System.out.println(Arrays.toString(columnNames));
                    DB.createIndex(tableName, columnNames);
                }catch(Exception ignored){
                    error=true;
                }
            }
            @Override
            public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
                try{
                    String tableName = ctx.table_name().getText();
                    Hashtable<String,String> htblColNameType = new Hashtable<>();

                    String clustringCol = "";
                    for (SQLiteParser.Column_defContext x : ctx.column_def()) {
                        if(x.column_constraint().size()>0){
                            if(x.column_constraint().get(0).getText().equalsIgnoreCase("primarykey")){
                                clustringCol = x.column_name().getText();
                            }else{
                                // only primary key in handled in this project
                                throw new Exception();
                            }
                        }
                        String dataType="";
                        if(x.type_name().getText().equalsIgnoreCase("int")){
                            dataType = "java.lang.integer";
                        }
                        if(x.type_name().getText().equalsIgnoreCase("double")){
                            dataType = "java.lang.double";
                        }
                        if(x.type_name().getText().equalsIgnoreCase("date")){
                            dataType = "java.util.date";
                        }
                        Pattern pattern = Pattern.compile("varchar(.*)", Pattern.CASE_INSENSITIVE);
                        Matcher matcher = pattern.matcher(x.type_name().getText());
                        if(matcher.find()) {
                            dataType = "java.lang.String";
                        }
                        htblColNameType.put(x.column_name().getText(),dataType);
                    }
                    Hashtable<String,String> min = new Hashtable<>();
                    Hashtable<String,String> max = new Hashtable<>();
                    for (SQLiteParser.Column_defContext x : ctx.column_def()) {
                        min.put(x.column_name().getText(),getMin(x.type_name().getText()));
                        max.put(x.column_name().getText(),getMax(x.type_name().getText()));
                        System.out.println(x.column_name().getText()+" "+x.type_name().getText());
                    }
                    DB.createTable(tableName,clustringCol,htblColNameType,min,max);
                }catch (Exception ignored){
                    error=true;
                }
            }

            @Override
            public void enterUpdate_stmt(SQLiteParser.Update_stmtContext ctx) {
                try{
                    String tableName = ctx.qualified_table_name().getText();
                    System.out.println(tableName);
                    Hashtable<String, Object> htblColNameValue = new Hashtable<>();
                    int size = ctx.column_name().size() ;
                    for(int i = 0; i < size; i++) {
                        String columnName = ctx.column_name().get(i).getText();
                        htblColNameValue.put(columnName, getObjectValue(tableName, columnName, ctx.expr().get(i).getText()));
                    }
                    String strClusteringKeyValue = ctx.expr(size).expr().get(1).getText();
//                    System.out.println(strClusteringKeyValue);
//                    System.out.println(htblColNameValue);
                    DB.updateTable(tableName, strClusteringKeyValue, htblColNameValue);

                }catch (Exception ignored){
                    error = true;
                }
            }

            @Override
            public void enterInsert_stmt(SQLiteParser.Insert_stmtContext ctx){
                try{
                    String tableName = ctx.table_name().getText() ;
                    Hashtable<String,Object> htblColNameValue = new Hashtable<>();
                    for(int i=0;i<ctx.column_name().size();i++){
                        String colName = ctx.column_name().get(i).getText();
                        htblColNameValue.put(colName,getObjectValue(tableName,colName,ctx.expr().get(i).getText()));
                    }
//                    System.out.println(htblColNameValue);
                    System.out.println("here");
                    DB.insertIntoTable(tableName,htblColNameValue);
                }catch (Exception ignored){
                    error = true;
                }
            }
            @Override
            public void enterDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {
                try{

                }catch (Exception ignored){
                    error = true;
                }
            }

        },tree);

        if(error){
            throw new DBAppException("Wrong SQl statement");
        }
        return it;
    }

    private static String getMin(String type) {
        if(type.equalsIgnoreCase("int")){
            return Integer.MIN_VALUE+"";
        }
        if(type.equalsIgnoreCase("double")){
            return Double.MIN_VALUE+"";
        }
        if(type.equalsIgnoreCase("date")){
            //TODO test this
            return new Date(0)+"";
        }else{
            return " ";
        }
    }
    private static String getMax(String type) {
        if(type.equalsIgnoreCase("int")){
            return Integer.MAX_VALUE+"";
        }
        if(type.equalsIgnoreCase("double")){
            return Double.MAX_VALUE+"";
        }
        if(type.equalsIgnoreCase("date")){
            //TODO add max date
            return new Date(0)+"";
        }else{
            String s = "";
            for(int i=0;i< type.toCharArray().length;i++){
                char c = type.toCharArray()[i];
                if(c == '('){
                    while(type.toCharArray()[i+1]!=')'){
                        i++;
                        s+=type.toCharArray()[i];
                    }
                }
            }
            int len = Integer.parseInt(s);
            //~ is the last printable char in ASCII
            return "~".repeat(len);
        }
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
        type = type.toLowerCase();
        switch (type) {
            case "java.lang.string" : o = value.substring(1,value.length()-1);break;
            case "java.lang.integer": o = Integer.parseInt(value);break;
            case "java.lang.double" :o = Double.parseDouble(value);break;
            case "java.util.date"   :
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




