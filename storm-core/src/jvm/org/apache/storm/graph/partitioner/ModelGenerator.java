package org.apache.storm.graph.partitioner;

import org.apache.storm.graph.Graph;
import org.apache.storm.graph.Vertex;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.SchedulingState;

/**
 * Created by amir on 5/7/17.
 */
public class ModelGenerator {
    int[] R1;
    int[] R2;

    int[] M1;
    int[] M2;

    int[][] Adjacency;

    public ModelGenerator() {
    }

    public Model generateModel(Graph g, SchedulingState state) throws Exception {
        RAS_Nodes nodes = state.nodes;

        M1 = new int[nodes.getNodes().size()];
        M2 = new int[nodes.getNodes().size()];

        R1 = new int[g.numVertices()];
        R2 = new int[g.numVertices()];
        Adjacency = new int[g.numVertices()][g.numVertices()];


        int cpuUsage;
        int memUsage;
        int i = 0;
        for (Vertex vertex :
                g.getVertices()) {
            cpuUsage = vertex.getWeights().getCpu();
            memUsage = vertex.getWeights().getMemory();
            R1[vertex.getId() - 1] = cpuUsage;
            R2[vertex.getId() - 1] = memUsage;
            for (Vertex neighbour :
                    g.getNeighbours(vertex)) {
                if (vertex.getId() < neighbour.getId()) {
                    int weight = g.getEdge(vertex.getName(), neighbour.getName()).getWeight();
                    if (weight == -1)
                        weight = 1;
                    Adjacency[(vertex.getId() - 1)][(neighbour.getId() - 1)] = weight;
                }
            }
        }
        i = 0;
        for (RAS_Node node :
                nodes.getNodes()) {
            int CPUUsedByTopology = 0;
            int MEMUsedByTopology = 0;
            if (node.getUsedSlots().size() > 0) {
                for (WorkerSlot ws :
                        node.getUsedSlots(g.getTopId())) {
                    CPUUsedByTopology = (int) node.getCpuUsedByWorker(ws);
                    MEMUsedByTopology = (int) node.getMemoryUsedByWorker(ws);
                }
            }
            //Available resources in the cluster + used resources by topology(because after repartitioning it can be uses again)
            //but resources used by other topologies cant be used by partitioner
            M1[i] = node.getAvailableCpuResources().intValue() + CPUUsedByTopology;
            M2[i++] = node.getAvailableMemoryResources().intValue() + MEMUsedByTopology;

        }

        return new Model(R1, R2, M1, M2, Adjacency);

    }

    public Model getModel1() throws Exception {
        R1 = new int[]{120, 100, 60, 120, 200};
        R2 = new int[]{1000, 1500, 2000, 250, 1000};

        M1 = new int[]{450, 200, 300};
        M2 = new int[]{4500, 800, 2000};

        int nTasks = R1.length;
        Adjacency = new int[nTasks][nTasks];
        Adjacency[0][2] = 10;
        Adjacency[0][3] = 7;
        Adjacency[1][3] = 12;
        Adjacency[2][4] = 20;
        Adjacency[3][4] = 15;

        Model model1 = new Model(R1, R2, M1, M2, Adjacency);
        return model1;
    }

    public Model getModel2() throws Exception {
        R1 = new int[]{100, 200, 150, 150, 250, 200};
        R2 = new int[]{200, 150, 100, 200, 200, 100};

        M1 = new int[]{800, 400, 500};
        M2 = new int[]{800, 500, 450};

        int nTasks = R1.length;
        Adjacency = new int[nTasks][nTasks];
        Adjacency[0][2] = 10;
        Adjacency[0][3] = 15;

        Adjacency[1][3] = 10;
        Adjacency[1][4] = 20;

        Adjacency[2][5] = 10;
        Adjacency[3][5] = 15;
        Adjacency[4][5] = 10;

        Model model2 = new Model(R1, R2, M1, M2, Adjacency);
        return model2;
    }

    public Model getModel3() throws Exception {
        R1 = new int[]{100, 200, 150, 150, 150, 100};
        R2 = new int[]{200, 150, 100, 200, 100, 100};

        M1 = new int[]{450, 500, 300};

        M2 = new int[]{600, 400, 450};

        int nTasks = R1.length;
        Adjacency = new int[nTasks][nTasks];
        Adjacency[0][2] = 10;
        Adjacency[0][3] = 10;

        Adjacency[1][3] = 10;
        Adjacency[1][4] = 15;

        Adjacency[2][5] = 5;
        Adjacency[3][5] = 5;
        Adjacency[4][5] = 20;

        Model model3 = new Model(R1, R2, M1, M2, Adjacency);
        return model3;
    }

    public Model getModel4() throws Exception {
        R1 = new int[]{100, 150, 200, 150, 150, 200, 200, 150};
        R2 = new int[]{150, 150, 100, 150, 100, 200, 200, 200};

        M1 = new int[]{500, 300, 600};
        M2 = new int[]{500, 300, 700};

        int nTasks = R1.length;
        Adjacency = new int[nTasks][nTasks];
        Adjacency[0][2] = 20;
        Adjacency[0][3] = 25;
        Adjacency[0][4] = 2;
        Adjacency[1][4] = 30;

        Adjacency[2][5] = 2;
        Adjacency[3][5] = 3;
        Adjacency[3][6] = 5;
        Adjacency[4][6] = 5;

        Adjacency[5][7] = 30;
        Adjacency[6][7] = 25;
        Model model4 = new Model(R1, R2, M1, M2, Adjacency);
        return model4;
    }

    public Model getModel5() throws Exception {
        R1 = new int[]{100, 150, 200, 150, 150, 200, 200, 150, 200, 100, 100, 150, 200, 150, 150, 200, 200, 150, 200, 100};
        R2 = new int[]{150, 150, 100, 150, 100, 200, 200, 200, 150, 200, 150, 150, 100, 150, 100, 200, 200, 200, 150, 200};

        M1 = new int[]{500, 300, 600, 400, 300, 600, 400, 300, 500, 700};
        M2 = new int[]{500, 300, 700, 300, 600, 400, 300, 500, 400, 300};

        int nTasks = R1.length;
        Adjacency = new int[nTasks][nTasks];

        for (int i = 0; i < nTasks; i++) {
            for (int j = 0; j < nTasks; j++) {
                if ((i < j)) {
                    if (Math.round(Math.random() * 1) == 1)
                        Adjacency[i][j] = 1;
                    else
                        Adjacency[i][j] = 0;
                }

            }
        }
        Model model5 = new Model(R1, R2, M1, M2, Adjacency);
        return model5;
    }
}
