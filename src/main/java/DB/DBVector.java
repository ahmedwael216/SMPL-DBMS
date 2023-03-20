package DB;

import java.util.Vector;

public class DBVector<T> extends Vector {
    private Vector<T> v;
    public DBVector(){
        this.v = new Vector<T>();
    }
}
