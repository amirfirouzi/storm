package org.apache.storm.graph.partitioner;

/**
 * Created by amir on 5/7/17.
 */
public class Model {

  private int nTasks;
  private int nMachines;

  private int[] R1;
  private int[] R2;

  private int[] M1;
  private int[] M2;

  private int[][] Adjacency;


  public Model(int[] reqResources1, int[] reqResources2, int[] availResources1, int[] availResources2, int[][] adjacency) throws Exception {
    if (reqResources1.length != reqResources2.length)
      throw new Exception("R1 & R2 sizes must be equal");
    else
      nTasks = reqResources1.length;

    if (availResources1.length != availResources2.length)
      throw new Exception("M1 & M2 sizes must be equal");
    else
      nMachines = availResources1.length;

    //Resource Demand
    //===============
    //Requested Resources(type 1:CPU, type 2: RAM) by tasks
    R1 = reqResources1;
    R2 = reqResources2;

    //Resource Pool
    //===============
    //Available Resources(type 1) by Machines
    M1 = availResources1;
    M2 = availResources2;

    //Adjacency Matrix containing communication links with weights
    Adjacency = adjacency;
  }

  public int getnTasks() {
    return nTasks;
  }

  public int getnMachines() {
    return nMachines;
  }

  public int[] getR1() {
    return R1;
  }

  public void setR1(int[] r1) {
    R1 = r1;
  }

  public int[] getR2() {
    return R2;
  }

  public void setR2(int[] r2) {
    R2 = r2;
  }

  public int[] getM1() {
    return M1;
  }

  public void setM1(int[] m1) {
    M1 = m1;
  }

  public int[] getM2() {
    return M2;
  }

  public void setM2(int[] m2) {
    M2 = m2;
  }

  public int[][] getAdjacency() {
    return Adjacency;
  }

  public void setAdjacency(int[][] adjacency) {
    Adjacency = adjacency;
  }
}
