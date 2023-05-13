package DB;

public class Point3D<T> {
    private Comparable xDim;
    private Comparable yDim;
    private Comparable zDim;
    private DBVector<Integer> references;

    public Point3D(Comparable xDim, Comparable yDim, Comparable zDim) {
        this.xDim = xDim;
        this.yDim = yDim;
        this.zDim = zDim;
        this.references = new DBVector<Integer>();
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

    public DBVector<Integer> getReferences() {
        return references;
    }

    public void addReference(Integer pageNumber) {
        references.add(pageNumber);
    }

    public void removeReference(Integer pageName) {
        references.remove(pageName);
    }

    public boolean equals(Point3D<T> point) {
        return this.xDim.equals(point.xDim) && this.yDim.equals(point.yDim) && this.zDim.equals(point.zDim);
    }

    public String toString() {
        return "(" + xDim + ", " + yDim + ", " + zDim + ")";
    }

}
