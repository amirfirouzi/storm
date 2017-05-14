package org.apache.storm.graph;

/**
 * Created by amir on 5/14/17.
 */
public class Resource {
    private String name;
    private int cpu;
    private int mem;

    public Resource(String name, int CPU, int Memory) {
        this.name = name;
        this.cpu = CPU;
        this.mem = Memory;
    }

    public Resource() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public int getMemory() {
        return mem;
    }

    public void setMemory(int mem) {
        this.mem = mem;
    }
}
