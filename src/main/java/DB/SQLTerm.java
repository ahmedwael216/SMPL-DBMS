package DB;

public class SQLTerm {
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;
    public SQLTerm(){}


    @Override
    public String toString() {
        return "SQLTerm{" +
                "TableName:" + _strTableName +
                ", ColumnName:" + _strColumnName +
                ", Operator:" + _strOperator +
                ", objValue:" + _objValue +
                '}';
    }
}
