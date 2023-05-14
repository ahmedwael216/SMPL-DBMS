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
        assertEquals(0,root.points.size());
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
    void deleteRemovesSinglePointFromLeafNode() throws DBAppException {
        Point3D<Integer> point = new Point3D<>(1, 1, 1);

        root.insert(point, 1);
        root.insert(point, 2);
        root.printComplete();
        root.delete(point, true,1);

        assertTrue(root.points.contains(point));
        assertEquals(1,root.points.get(0).getReferences().size());
    }

    @Test
    @Order(7)
    void deleteRemovesDuplicatesFromLeafNode() throws DBAppException {
        Point3D<Integer> point = new Point3D<>(1, 1, 1);

        root.insert(point, 1);
        root.insert(point, 2);
        root.delete(point, false,1); // here page number doesn't matter as all the references will be deleted.
        assertFalse(root.points.contains(point));

        Assertions.assertThrows(DBAppException.class, () -> {
            root.delete(point,false, 2); // // here page number doesn't matter as all the references will be deleted.
        });


    }

    @Test
    @Order(8)
    void deleteRemovesMultiplePointsFromLeafNode() throws DBAppException {
        root.insert(new Point3D<>(1, 1, 1), 1);
        root.insert(new Point3D<>(2, 2, 2), 2);
        root.insert(new Point3D<>(3, 3, 3), 3);
        root.insert(new Point3D<>(4, 4, 4), 4);
        root.insert(new Point3D<>(3, 5, 5), 6);
        root.insert(new Point3D<>(5, 3, 5), 7);
        root.insert(new Point3D<>(8, 8, 4), 8);
        root.insert(new Point3D<>(5, 5, 5), 9);
        root.insert(new Point3D<>(8, 8, 8), 10);

        root.delete(new Point3D<>(1, 1, 1),true,1);
        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(1, 1, 1)));
        root.delete(new Point3D<>(3, 5, 5),true,6);
        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(3, 5, 5)));
        root.delete(new Point3D<>(4, 4, 4),true,4);
        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(4, 4, 4)));
        root.delete(new Point3D<>(8, 8, 8),true,10);
        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(8, 8, 8)));
        root.delete(new Point3D<>(5, 3, 5),true,7);
        assertFalse(root.getChildren()[0].points.contains(new Point3D<Integer>(5, 1, 5)));

        assertEquals(8,root.getChildren().length);


    }

    @Test
    @Order(9)
    void deleteMergesChildrenWhenAllChildrenAreEmpty() throws DBAppException {
        root.insert(new Point3D<>(1, 1, 1), 1);
        root.insert(new Point3D<>(2, 2, 2), 2);
        root.insert(new Point3D<>(3, 3, 3), 3);
        root.insert(new Point3D<>(4, 4, 4), 4);
        root.insert(new Point3D<>(3, 5, 5), 6); // causes split for the root
        assertEquals(8,root.getChildren().length);

        root.insert(new Point3D<>(2, 3, 4), 7); // causes split to first child
        assertEquals(8,root.getChildren()[0].getChildren().length);

        root.getChildren()[0].delete(new Point3D<>(1, 1, 1),false,1);
        root.getChildren()[0].delete(new Point3D<>(2, 2, 2),false,2);
        root.getChildren()[0].delete(new Point3D<>(3, 3, 3),false,3);
        root.getChildren()[0].delete(new Point3D<>(4, 4, 4),false,4);
        assertEquals(8,root.getChildren()[0].getChildren().length);
        root.getChildren()[0].delete(new Point3D<>(2, 3, 4),false,7); // causes merge to first child children

        assertNull(root.getChildren()[0].getChildren());
        root.delete(new Point3D<>(3, 5, 5),false,6); // cause merge to root children
        assertNull(root.getChildren());

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
            root.delete(new Point3D<>(6, 6, 6),true,6);
        });

    }

    @Test
    @Order(11)
    void deleteThrowsExceptionInValidPointRange() throws DBAppException {
        root.insert(new Point3D<>(1, 1, 1), 1);
        root.insert(new Point3D<>(2, 2, 2), 2);
        root.insert(new Point3D<>(3, 3, 3), 3);
        root.insert(new Point3D<>(4, 4, 4), 4);
        root.insert(new Point3D<>(5, 5, 5), 5);


        Assertions.assertThrows(DBAppException.class, () -> {
            root.delete(new Point3D<>(100, 100, 100),true,6);
        });

    }

    @Test
    @Order(12)
    void updateReplacesSinglePageNumberInPoint() throws DBAppException {
        Point3D<Integer> point = new Point3D<>(1, 1, 1);

        root.insert(point, 1);
        root.insert(point,2);
        root.insert(point,3);
        root.update(point,true, 2, 5);

        assertTrue(root.points.get(0).getReferences().contains(1));
        assertTrue(root.points.get(0).getReferences().contains(3));
        assertFalse(root.points.get(0).getReferences().contains(2));
        assertTrue(root.points.get(0).getReferences().contains(5));

    }

    @Test
    @Order(13)
    void updateReplacesMultiplePageNumberInPoint() throws DBAppException {
        Point3D<Integer> point = new Point3D<>(1, 1, 1);

        root.insert(point, 1);
        root.insert(point,2);
        root.insert(point,2);
        root.insert(point,3);
        root.update(point,false, 2, 5); // replace all occurrences of 2 with 5

        assertTrue(root.points.get(0).getReferences().contains(1));
        assertTrue(root.points.get(0).getReferences().contains(3));
        assertFalse(root.points.get(0).getReferences().contains(2));
        assertTrue(root.points.get(0).getReferences().contains(5));
    }

    @Test
    @Order(14)
    void updateThrowsExceptionForNoneExistingPoint() throws DBAppException {
        Assertions.assertThrows(DBAppException.class, () -> {
            root.update(new Point3D<>(100, 100, 100),false, 2, 5);
        });
    }

}