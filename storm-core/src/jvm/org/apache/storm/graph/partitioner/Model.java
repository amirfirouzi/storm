package org.apache.storm.graph.partitioner;

/**
 * Created by amir on 5/7/17.
 */
public class Model {

    private int nTasks;
    private int nMachines;

    private int[] ResCPU;
    private int[] ResMEM;

    private int[] CapCPU;
    private int[] CapMEM;

    private int[][] Adjacency;


    public Model(int[] reqResourcesCPU, int[] reqResourcesMEM, int[] availResourcesCPU, int[] availResourcesMEM, int[][] adjacency) throws Exception {
        if (reqResourcesCPU.length != reqResourcesMEM.length)
            throw new Exception("ResCPU & ResMEM sizes must be equal");
        else
            nTasks = reqResourcesCPU.length;

        if (availResourcesCPU.length != availResourcesMEM.length)
            throw new Exception("CapCPU & CapMEM sizes must be equal");
        else
            nMachines = availResourcesCPU.length;

        //Resource Demand
        //===============
        //Requested Resources(type 1:CPU, type 2: RAM) by tasks
        ResCPU = reqResourcesCPU;
        ResMEM = reqResourcesMEM;

        //Resource Pool
        //===============
        //Available Resources(type 1) by Machines
        CapCPU = availResourcesCPU;
        CapMEM = availResourcesMEM;

        //Adjacency Matrix containing communication links with weights
        Adjacency = adjacency;
    }

    public int getnTasks() {
        return nTasks;
    }

    public int getnMachines() {
        return nMachines;
    }

    public int[] getResCPU() {
        return ResCPU;
    }

    public void setResCPU(int[] resCPU) {
        ResCPU = resCPU;
    }

    public int[] getResMEM() {
        return ResMEM;
    }

    public void setResMEM(int[] resMEM) {
        ResMEM = resMEM;
    }

    public int[] getCapCPU() {
        return CapCPU;
    }

    public void setCapCPU(int[] capCPU) {
        CapCPU = capCPU;
    }

    public int[] getCapMEM() {
        return CapMEM;
    }

    public void setCapMEM(int[] capMEM) {
        CapMEM = capMEM;
    }

    public int[][] getAdjacency() {
        return Adjacency;
    }

    public void setAdjacency(int[][] adjacency) {
        Adjacency = adjacency;
    }
}
