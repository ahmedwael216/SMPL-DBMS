package DB;

public class Node<T> {
    private Node[] children;
    DimRange xRange;
    DimRange yRange;
    DimRange zRange;
    private static int maxCapacity;
    DBVector<Point3D> points;

    public Node(Comparable minX, Comparable minY, Comparable minZ, Comparable maxX, Comparable maxY, Comparable maxZ) {
        this.xRange = new DimRange(minX, maxX);
        this.yRange = new DimRange(minY, maxY);
        this.zRange = new DimRange(minZ, maxZ);
        this.maxCapacity = DBApp.maxEntriesCountNode;
        this.points = new DBVector<Point3D>();
    }

    public void insert() {

    }

    public DBVector<String> search() {
        return null;
    }

    public void delete() {

    }

}
