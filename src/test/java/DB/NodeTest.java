package DB;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class NodeTest {

    Node<Integer> root;

    @BeforeEach
    void setUp() {
        root = new Node<>(0,0,0,8,8,8);
    }

    @Test
    @Order(1)
    void insertAddsPointToLeafNode() throws DBAppException {
        Point3D<Integer> point = new Point3D<>(1, 1, 1);
        int pageNumber = 1;

        root.insert(point, pageNumber);

        assertTrue(root.points.contains(point));
        assertEquals(pageNumber, point.getReferences().get(0));
    }

    @Test
    @Order(2)
    void insertDoesNotDuplicatePointsInLeafNodeButReferences() throws DBAppException {
        Point3D point = new Point3D(1, 1, 1);
        int pageNumber = 1;

        root.insert(point, pageNumber);
        root.insert(point, pageNumber);

        assertEquals(1, root.points.size());
        assertEquals(2, point.getReferences().size());
    }

    @Test
    @Order(3)
    void insertSplitsLeafNodeWhenCapacityExceeded() throws DBAppException {
        root.insert(new Point3D<Integer>(1, 1, 1), 1);
        root.insert(new Point3D<Integer>(2, 2, 2), 2);
        root.insert(new Point3D<Integer>(3, 3, 3), 3);
        root.insert(new Point3D<Integer>(4, 4, 4), 4);
        root.insert(new Point3D<Integer>(5, 5, 5), 5);

        assertNotNull(root.getChildren());
        assertEquals(8, root.getChildren().length);
    }

    @Test
    @Order(4)
    void insertThrowsExceptionForInvalidRangePoint() throws DBAppException {
        Assertions.assertThrows(DBAppException.class, () -> {
            root.insert(new Point3D<Integer>(7, 8, 9), 1);
        });
    }

    @Test
    @Order(5)
    void searchReturnsCorrectPageNumbers() throws DBAppException {
        root.insert(new Point3D<>(1, 1, 1), 1);
        root.insert(new Point3D<>(2, 2, 2), 2);
        root.insert(new Point3D<>(3, 3, 3), 3);
        root.insert(new Point3D<>(4, 4, 4), 4);
        root.insert(new Point3D<>(5, 5, 5), 5);

        DimRange x = new DimRange(1, 3);
        DimRange y = new DimRange(1, 3);
        DimRange z = new DimRange(1, 3);

        DBVector<Integer> result = root.search(x, y, z);

        assertEquals(3, result.size());
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));
    }

    @Test
    @Order(6)
    void deleteRemovesPointFromLeafNode() throws DBAppException {
        Point3D<Integer> point = new Point3D<>(1, 1, 1);
        int pageNumber = 1;

        root.insert(point, pageNumber);
        root.delete(point, pageNumber);

        assertFalse(root.points.contains(point));
    }

    @Test
    @Order(7)
    void deleteRemovesMultiplePointsFromLeafNode() throws DBAppException {
        Point3D<Integer> point1 = new Point3D<>(1, 1, 1);
        Point3D<Integer> point2 = new Point3D<>(2, 2, 2);
        int pageNumber = 1;

        root.insert(point1, pageNumber);
        root.insert(point2, pageNumber);
        root.delete(point1, pageNumber);
        root.delete(point2, pageNumber);

        assertFalse(root.points.contains(point1));
        assertFalse(root.points.contains(point2));
    }

    @Test
    @Order(8)
    void deleteRemovesPointFromChildNode() throws DBAppException {
        root.insert(new Point3D<>(1, 1, 1), 1);
        root.insert(new Point3D<>(2, 2, 2), 2);
        root.insert(new Point3D<>(3, 3, 3), 3);
        root.insert(new Point3D<>(4, 4, 4), 4);
        root.insert(new Point3D<>(5, 5, 5), 1);

        root.delete(new Point3D<>(1, 1, 1), 1);

        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(1, 1, 1)));
    }

    @Test
    @Order(9)
    void deleteRemovesMultiplePointsFromChildNode() throws DBAppException {
        root.insert(new Point3D<>(1, 1, 1), 1);
        root.insert(new Point3D<>(2, 2, 2), 2);
        root.insert(new Point3D<>(3, 3, 3), 3);
        root.insert(new Point3D<>(4, 4, 4), 4);
        root.insert(new Point3D<>(5, 5, 5), 1);

        root.delete(new Point3D<>(1, 1, 1), 1);
        root.delete(new Point3D<>(2, 2, 2), 2);
        root.delete(new Point3D<>(3, 3, 3), 3);
        root.delete(new Point3D<>(4, 4, 4), 4);

        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(1, 1, 1)));
        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(2, 2, 2)));
        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(3, 3, 3)));
        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(4, 4, 4)));
    }

    @Test
    @Order(10)
    void deleteThrowsExceptionForNonExistingPoint() throws DBAppException {
        root.insert(new Point3D<>(1, 1, 1), 1);
        root.insert(new Point3D<>(2, 2, 2), 2);
        root.insert(new Point3D<>(3, 3, 3), 3);
        root.insert(new Point3D<>(4, 4, 4), 4);
        root.insert(new Point3D<>(5, 5, 5), 5);

        Assertions.assertThrows(DBAppException.class, () -> {
            root.delete(new Point3D<>(6, 6, 6), 6);
        });


    }

    @Test
    void update() {
    }


}