package org.example;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    @Test
    void add() {
        assertEquals(4, Main.add(2, 2));
    }
}