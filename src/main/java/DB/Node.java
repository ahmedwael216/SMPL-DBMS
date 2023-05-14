package DB;

import java.util.ArrayList;
import java.util.HashSet;

public class Node<T> {
    private Node<T>[] children;
    private DimRange xRange;
    private DimRange yRange;
    private DimRange zRange;
    private static int maxCapacity;
    DBVector<Point3D<T>> points;

    public Node(Comparable minX, Comparable minY, Comparable minZ, Comparable maxX, Comparable maxY, Comparable maxZ) {
        this.xRange = new DimRange(minX, maxX);
        this.yRange = new DimRange(minY, maxY);
        this.zRange = new DimRange(minZ, maxZ);
        this.maxCapacity = DBApp.maxEntriesCountNode;
        this.points = new DBVector<>();
    }

    public Node<T>[] getChildren() {
        return this.children;
    }

    public DimRange getXRange() {
        return this.xRange;
    }

    public DimRange getYRange() {
        return this.yRange;
    }

    public DimRange getZRange() {
        return this.zRange;
    }


    public void insert(Point3D point, int pageNumber) throws DBAppException {
        if (children == null) {  // this is a leaf
            int index = points.indexOf(point);
            if (index != -1) {
//                points.get(index).addReference(pageNumber);
                return;
            }
            points.add(point);
            point.addReference(pageNumber);
            if (points.size() > maxCapacity) {
                split();
            }
            return;
        }
        // search for the node to insert in

        for (int i = 0; i < children.length; i++) {
            if (children[i].inRange(point)) {
                children[i].insert(point, pageNumber);
                return;
            }
        }
        throw new DBAppException("The inserted point is out of valid range, the point: " + point.toString());
    }


    private void split() {
        DimRange[] newRangeX = xRange.split(), newRangeY = yRange.split(), newRangeZ = zRange.split();

        children = new Node[8];
        for (int i = 0; i < 8; i++) {
            children[i] = new Node(newRangeX[(i >> 2) & 1].getMin(), newRangeY[(i >> 1) & 1].getMin(), newRangeZ[i & 1].getMin(), newRangeX[(i >> 2) & 1].getMax(), newRangeY[(i >> 1) & 1].getMax(), newRangeZ[i & 1].getMax());
        }
        for (Point3D point : this.points) {
            for (int i = 0; i < 8; i++) {
                if (children[i].inRange(point)) {
                    children[i].points.add(point);
                    break;
                }
            }
        }

        this.points.clear();
    }

    public DBVector<Integer> search(DimRange x, DimRange y, DimRange z) {
        HashSet<Integer> temp = new HashSet<>(searchHelper(x, y, z));
        DBVector<Integer> res = new DBVector<>();
        res.addAll(temp);
        return res;
    }

    private DBVector<Integer> searchHelper(DimRange x, DimRange y, DimRange z) {
        if (children == null) {
            DBVector<Integer> result = new DBVector<>();
            for(Point3D point : points) {
                if(x.inRange(point.getXDim()) && y.inRange(point.getYDim()) && z.inRange(point.getZDim())) {
                    result.addAll(point.getReferences());
                }
            }
            return result;
        }

        DBVector<Integer> result = new DBVector<>();
        for (int i = 0; i < children.length; i++) {
            if (xRange.intersect(x) && yRange.intersect(y) && zRange.intersect(z)) {
                result.addAll(children[i].search(x, y, z));
            }
        }

        return result;
    }

    public void delete(Point3D point, int pageNumber) throws DBAppException {
        deleteHelper(point, null, pageNumber);
    }

    private boolean canBeMerged() {
        int size = 0;
        for (int i = 0; i < children.length; i++) {
            size += children[i].points.size();
        }

        return size <= maxCapacity;
    }

    private void merge() {
        for (int i = 0; i < children.length; i++) {
            points.addAll(children[i].points);
        }
        children = null;
    }

    private void deleteHelper(Point3D point, Node parent, int pageNumber) throws DBAppException {
        if (children == null) {
            int index = points.indexOf(point);
            if (index == -1) {
                throw new DBAppException("The point to be deleted is not found");
            }
            points.get(index).removeReference(pageNumber);
            if (points.get(index).getReferences().isEmpty()) {
                points.remove(index);
                if (parent != null && parent.canBeMerged()) {
                    parent.merge();
                }
            }
            return;
        }

        for (int i = 0; i < children.length; i++) {
            if (children[i].inRange(point)) {
                children[i].deleteHelper(point, this, pageNumber);
                return;
            }
        }
    }

    public void update(Point3D point, int oldPageNumber, int newPageNumnber) throws DBAppException {
        delete(point, oldPageNumber);
        insert(point, newPageNumnber);
    }

    public String toString() {
        return "Node{" +
                "xRange=" + xRange +
                ", yRange=" + yRange +
                ", zRange=" + zRange +
                ", points=" + points +
                '}';
    }

    private boolean inRange(Point3D point) {
        return xRange.inRange(point.getXDim()) && yRange.inRange(point.getYDim()) && zRange.inRange(point.getZDim());
    }

    private void printComplete() {
        if (this != null)
            System.out.println(this.toString());
        if (children == null) {
            return;
        }

        for (int i = 0; i < children.length; i++) {
            children[i].printComplete();
        }

    }

    public static void main(String[] args) throws DBAppException {
        Node root = new Node(0, 0, 0, 8, 8, 8);

        root.insert(new Point3D(1, 1, 1), 1);
        root.insert(new Point3D(2, 2, 2), 2);
        root.insert(new Point3D(3, 3, 3), 3);
        root.insert(new Point3D(4, 4, 4), 4);
        root.insert(new Point3D(5, 5, 5), 5);

        root.printComplete();

        root.delete(new Point3D(1, 1, 1), 1);

        System.out.println("-----------------------------");

        root.printComplete();

    }
}