package org.apache.storm.graph.partitioner;

import org.apache.storm.graph.Vertex;
import org.apache.storm.scheduler.resource.RAS_Node;

import java.util.List;

/**
 * Created by amir on 5/14/17.
 */
public class Partition {

    private RAS_Node node;
    private List<Vertex> vertices;
    private int loadR1;
    private int loadR2;

    public Partition(RAS_Node node, List<Vertex> vertices, int loadR1, int loadR2) {
        this.node = node;
        this.vertices = vertices;
        this.loadR1 = loadR1;
        this.loadR2 = loadR2;
    }

    public RAS_Node getNode() {
        return node;
    }

    public void setNode(RAS_Node node) {
        this.node = node;
    }

    public List<Vertex> getVertices() {
        return vertices;
    }

    public void setVertices(List<Vertex> vertices) {
        this.vertices = vertices;
    }

    public int getLoadR1() {
        return loadR1;
    }

    public void setLoadR1(int loadR1) {
        this.loadR1 = loadR1;
    }

    public int getLoadR2() {
        return loadR2;
    }

    public void setLoadR2(int loadR2) {
        this.loadR2 = loadR2;
    }
}
