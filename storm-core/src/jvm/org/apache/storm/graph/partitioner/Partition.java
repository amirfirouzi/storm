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
    private int loadCPU;
    private int loadMEM;
    private int capacityCPU;
    private int capacityMEM;

    public Partition(RAS_Node node, List<Vertex> vertices, int loadCPU, int loadMEM, int capacityCPU, int capacityMEM) {
        this.node = node;
        this.vertices = vertices;
        this.loadCPU = loadCPU;
        this.loadMEM = loadMEM;
        this.capacityCPU = capacityCPU;
        this.capacityMEM = capacityMEM;
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

    public int getLoadCPU() {
        return loadCPU;
    }

    public void setLoadCPU(int loadCPU) {
        this.loadCPU = loadCPU;
    }

    public int getLoadMEM() {
        return loadMEM;
    }

    public void setLoadMEM(int loadMEM) {
        this.loadMEM = loadMEM;
    }

    public int getCapacityCPU() {
        return capacityCPU;
    }

    public void setCapacityCPU(int capacityCPU) {
        this.capacityCPU = capacityCPU;
    }

    public int getCapacityMEM() {
        return capacityMEM;
    }

    public void setCapacityMEM(int capacityMEM) {
        this.capacityMEM = capacityMEM;
    }
}
