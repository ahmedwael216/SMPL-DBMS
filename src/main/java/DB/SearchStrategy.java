package DB;

import java.io.IOException;

public abstract class SearchStrategy {
    public static int getColIndex(String[] keys, String colName){
        for(int i=0;i<keys.length;i++){
            if(keys[i].equals(colName))return i;
        }
        return -1;

    }

    public static boolean expressionEval (String operator, Comparable obj1, Comparable obj2){
        int c = obj1.compareTo(obj2);
        switch (operator){
            case "=": return c==0;
            case ">": return c>0;
            case ">=": return c>=0;
            case  "<": return c<0;
            case "<=": return c<=0;
            case "!=": return c!=0;
            default:return false;
        }

    }
}
