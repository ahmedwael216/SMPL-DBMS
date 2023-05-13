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


    public void insert(Point3D point, int pageNumber) throws DBAppException {
        if(children == null){  // this is a leaf
            int index = points.indexOf(point);
            if(index != -1){
                points.get(index).addReference(pageNumber);
                return;
            }
            points.add(point);
            point.addReference(pageNumber);
            if(points.size() > maxCapacity){
                split();
            }
            return;
        }
        // search for the node to insert in

        for(int i = 0; i < children.length; i++){
            if(children[i].inRange(point)){
                children[i].insert(point, pageNumber);
                return;
            }
        }
        throw new DBAppException("The inserted point is out of valid range, the point: " + point.toString());
    }


    private void split(){
        DimRange[] newRangeX = xRange.split(), newRangeY = yRange.split(), newRangeZ = zRange.split();

        children = new Node[8];
        for(int i = 0; i < 8; i++){
            children[i] = new Node(newRangeX[(i >> 2) & 1].getMin(), newRangeY[(i >> 1) & 1].getMin(), newRangeZ[i & 1].getMin(), newRangeX[(i >> 2) & 1].getMax(), newRangeY[(i >> 1) & 1].getMax(), newRangeZ[i & 1].getMax());
        }
        for(Point3D point : this.points){
            for(int i = 0; i < 8; i++){
                if(children[i].inRange(point)){
                    children[i].points.add(point);
                    break;
                }
            }
        }

        this.points.clear();
    }
    public DBVector<Integer> search(Point3D point) {
        if(children == null){
            int index = points.indexOf(point);
            if(index == -1){
                return new DBVector<>();
            }
            return (DBVector<Integer>) points.get(index).getReferences().clone();
        }

        DBVector<Integer> result = new DBVector<>();
        for(int i = 0; i < children.length; i++){
            if(children[i].inRange(point)){
                result = children[i].search(point);
                break;
            }
        }

        return result;
    }

    public void delete(Point3D point) throws DBAppException {
        deleteHelper(point, null);
    }
    private boolean canBeMerged(){
        int size = 0;
        for(int i = 0; i < children.length; i++){
            size += children[i].points.size();
        }

        return size <= maxCapacity;
    }
    private void merge(){
        for(int i = 0; i < children.length; i++){
            points.addAll(children[i].points);
        }
        children = null;
    }
    private void deleteHelper(Point3D point, Node parent) throws DBAppException {
        if(children == null){
            int index = points.indexOf(point);
            if(index == -1){
                throw new DBAppException("The point to be deleted is not found");
            }
            points.remove(index);
            if(parent != null && parent.canBeMerged()){
                parent.merge();
            }
            return;
        }

        for(int i = 0; i < children.length; i++){
            if(children[i].inRange(point)){
                children[i].deleteHelper(point, this);
                return;
            }
        }
    }
    private boolean inRange(Point3D point) {
        return xRange.inRange(point.getXDim()) && yRange.inRange(point.getYDim()) && zRange.inRange(point.getZDim());
    }


}
