package org.example;

import java.io.Serializable;

public class MyPoint implements Serializable {
    private String id;
    private double x;
    private double y;

    public MyPoint(String id, double x, double y) {
        this.id = id;
        this.x = x;
        this.y = y;
    }

    public String getId() {
        return id;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }
}

