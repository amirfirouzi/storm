package org.apache.storm.scheduler.resource;

import org.apache.storm.graph.Vertex;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.WorkerSlot;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by amir on 6/21/17.
 */
public class SchedulingUtils {

    public static String verticesToString(List<Vertex> vertices, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("********************" + name + "\n");
        sb.append("\t");
        if (vertices.isEmpty()) {
            sb.append("NULL");
        } else {
            for (Vertex vertex :
                    vertices) {
                sb.append(vertex.getExecutor().toString() + "obj:" + System.identityHashCode(vertex.getExecutor()) + ", ");
            }
        }
        return sb.toString();
    }

    public static String execsToString(Collection<ExecutorDetails> execs, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("********************" + name + "\n");
        sb.append("\t");
        if (execs.isEmpty()) {
            sb.append("NULL");
        } else {
            for (ExecutorDetails exec :
                    execs) {
                sb.append(exec.toString() + "obj:" + System.identityHashCode(exec) + ", ");
            }
        }
        return sb.toString();
    }

    public static String assignmentToString(Map<WorkerSlot, Collection<ExecutorDetails>> assignments, String logTitle) {
        StringBuilder sb = new StringBuilder();
        sb.append("********************" + logTitle + "\n");
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> assignment :
                assignments.entrySet()) {
            sb.append("\tworker: " + assignment.getKey().toString() + ": \n");
            sb.append("\t\t");
            if (assignment.getValue().isEmpty()) {
                sb.append("NULL");
            } else {
                sb.append("Execs: ");
                for (ExecutorDetails exec :
                        assignment.getValue()) {
                    sb.append(exec.toString() + "obj:" + System.identityHashCode(exec) + ", ");
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public static void removeSimiliarElements(List<Vertex> list1, List<Vertex> list2) {
        for (Vertex item :
                list2) {
            removeFromListIfContains(list1, item.toString());
        }
    }

    private static void removeFromListIfContains(List<?> list, String itemName) {
        Iterator<?> iter = list.iterator();
        while (iter.hasNext()) {
            String item = iter.next().toString();
            if (item.equals(itemName)) {
                iter.remove();
            }
        }
    }

    public static boolean listContains(List<?> list, String itemName) {
        Iterator<?> iter = list.iterator();
        while (iter.hasNext()) {
            String item = iter.next().toString();
            if (item.equals(itemName)) {
                return true;
            }
        }
        return false;
    }
}
