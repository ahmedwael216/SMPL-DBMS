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

    @Override
    public boolean equals(Object p) {
        Point3D point = (Point3D) p;
        return this.xDim.compareTo(point.xDim) == 0 && this.yDim.compareTo(point.yDim) == 0 && this.zDim.compareTo(point.zDim) == 0;
    }

    @Override
    public String toString() {
        return "(" + xDim + ", " + yDim + ", " + zDim + ")";
    }

}
