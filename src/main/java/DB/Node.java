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
            if (!this.xRange.inRange(point.getXDim()) || !this.yRange.inRange(point.getYDim()) || !this.zRange.inRange(point.getZDim()))
                throw new DBAppException("The inserted point is out of valid range, the point: " + point.toString());
            int index = points.indexOf(point);
            if (index != -1) {
                points.get(index).addReference(pageNumber);
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
            for (Point3D point : points) {
                if (x.inRange(point.getXDim()) && y.inRange(point.getYDim()) && z.inRange(point.getZDim())) {
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

    public void delete(Point3D<T> point, boolean deleteSingle, int pageNumber) throws DBAppException {
        Node<T> leaf = getLeaf(point);
        if (leaf == null)
            return;
        int index = leaf.points.indexOf(point);
        if (index == -1) {
            System.out.println(point.toString());
            throw new DBAppException("The point to be deleted is not found");
        }

        for (int i = 0; i < leaf.points.size(); i++) {
            Point3D<T> p = leaf.points.get(i);
            if (p.getXDim().equals(point.getXDim()) && p.getYDim().equals(point.getYDim()) && p.getZDim().equals(point.getZDim())) {
                if (deleteSingle) {
                    p.removeReference(pageNumber);
                    if (p.getReferences().size() == 0)
                        leaf.points.remove(i);
                } else {
                    leaf.points.remove(i);
                }

                if(children == null)
                    continue;

                boolean emptyChildren = true;
                for (Node<T> child : children) {
                    if (child.children != null) {
                        emptyChildren = false;
                        break;
                    }
                    if (child.points.size() != 0) {
                        emptyChildren = false;
                        break;
                    }
                }

                if (emptyChildren)
                    children = null;

                return;
            }
        }

    }


    private Node<T> getLeaf(Point3D<T> point) throws DBAppException {
        if (children == null) {
            return this;
        }

        for (int i = 0; i < children.length; i++) {
            if (children[i].inRange(point)) {
                System.out.println("Child: " + children[i].toString() + " " + children[i].getChildren());
                Node<T> res = children[i].getLeaf(point);

                return res;
            }
        }
        return null;
    }

    public void update(Point3D<T> point,boolean updateSingle, int oldPageNumber, int newPageNumber) throws DBAppException {
        Node<T> leaf = getLeaf(point);
        if(leaf == null){
            return;
        }

        for(int i = 0; i < leaf.points.size(); i++){
            Point3D<T> p = leaf.points.get(i);
            if(p.getXDim().equals(point.getXDim()) && p.getYDim().equals(point.getYDim()) && p.getZDim().equals(point.getZDim())){
                if(updateSingle){
                    p.removeReference(oldPageNumber);
                    p.addReference(newPageNumber);
                }
                else{
                   for(int j = 0; j < p.getReferences().size(); j++){
                       if(p.getReferences().get(j) == oldPageNumber){
                           p.getReferences().set(j,newPageNumber);
                       }
                   }
                }
                return;
            }
        }
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

    public void printComplete() {
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
        root.insert(new Point3D(3, 5, 5), 5);
        root.insert(new Point3D(5, 5, 5), 5);

        root.printComplete();

        root.delete(new Point3D(1, 1, 1),true,1);
        root.delete(new Point3D(2,2, 2),true,2);
        root.delete(new Point3D(3, 3, 3),true,3);
        root.delete(new Point3D(4, 4, 4),true,4);
        root.delete(new Point3D(3, 5, 5),true,5);
        root.delete(new Point3D(5, 5, 5),true,5);

        System.out.println("-----------------------------");

        root.printComplete();
    }
}