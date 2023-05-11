package DB;

public class Node {
    private Node[] children;
    DimRange xRange;
    DimRange yRange;
    DimRange zRange;

    private static int maxCapcity;

    public Node(Comparable minX, Comparable minY, Comparable minZ, Comparable maxX, Comparable maxY, Comparable maxZ) {
        this.xRange = new DimRange(minX, maxX);
        this.yRange = new DimRange(minY, maxY);
        this.zRange = new DimRange(minZ, maxZ);
        this.maxCapcity = DBApp.maxEntriesCountNode;
    }

    public void insert() {

    }

    public DBVector<String> search() {
        return null;
    }

    public void delete() {

    }

}
