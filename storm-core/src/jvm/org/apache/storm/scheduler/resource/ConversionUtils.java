package org.apache.storm.scheduler.resource;

import org.apache.storm.graph.Vertex;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.WorkerSlot;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by amir on 6/21/17.
 */
public class ConversionUtils {

    public static String verticesToString(List<Vertex> vertices, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("********************" + name + "********************\n");
        sb.append("\t");
        if (vertices.isEmpty()) {
            sb.append("NULL");
        } else {
            for (Vertex vertex :
                    vertices) {
                sb.append(vertex.getExecutor().toString() + ", ");
            }
        }
        return sb.toString();
    }

    public static String execsToString(Collection<ExecutorDetails> execs, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("********************" + name + "********************\n");
        sb.append("\t");
        if (execs.isEmpty()) {
            sb.append("NULL");
        } else {
            for (ExecutorDetails exec :
                    execs) {
                sb.append(exec.toString() + ", ");
            }
        }
        return sb.toString();
    }

    public static String assignmentToString(Map<WorkerSlot, Collection<ExecutorDetails>> assignments, String logTitle) {
        StringBuilder sb = new StringBuilder();
        sb.append("********************" + logTitle + "********************\n");
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> assignment :
                assignments.entrySet()) {
            sb.append("\tworker: " + assignment.getKey().toString() + ": \n");
            sb.append("\t\t");
            if (assignment.getValue().isEmpty()) {
                sb.append("NULL");
            } else {
                for (ExecutorDetails exec :
                        assignment.getValue()) {
                    sb.append("Execs: " + exec.toString() + ", ");
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
