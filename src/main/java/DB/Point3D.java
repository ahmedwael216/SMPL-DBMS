package DB;

public class Point3D<T> {
    private Comparable xDim;
    private Comparable yDim;
    private Comparable zDim;
    private DBVector<String> references;

    public Point3D(Comparable xDim, Comparable yDim, Comparable zDim) {
        this.xDim = xDim;
        this.yDim = yDim;
        this.zDim = zDim;
        this.references = new DBVector<String>();
    }

    public Comparable getXDim() {
        return xDim;
    }

    public Comparable getYDim() {
        return yDim;
    }

    public Comparable getZDim() {
        return zDim;
    }

    public DBVector<String> getReferences() {
        return references;
    }

    public void addReference(String pageName) {
        references.add(pageName);
    }

}
