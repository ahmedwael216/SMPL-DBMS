package DB;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

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
    void insertDoesNotDuplicatePointsInLeafNode() throws DBAppException {
        Point3D point = new Point3D(1, 1, 1);
        int pageNumber = 1;

        root.insert(point, pageNumber);
        root.insert(point, pageNumber);

        assertEquals(1, root.points.size());
        assertEquals(1, point.getReferences().size());
    }

    @Test
    @Order(3)
    void insert_SplitsLeafNodeWhenCapacityExceeded() throws DBAppException {
        root.insert(new Point3D<Integer>(1, 1, 1), 1);
        root.insert(new Point3D<Integer>(2, 2, 2), 2);
        root.insert(new Point3D<Integer>(3, 3, 3), 3);
        root.insert(new Point3D<Integer>(4, 4, 4), 4);
        root.insert(new Point3D<Integer>(5, 5, 5), 5);

        assertNotNull(root.getChildren());
        assertEquals(8, root.getChildren().length);
    }

    @Test
    void search() {
    }

    @Test
    void delete() {
    }

    @Test
    void update() {
    }


}