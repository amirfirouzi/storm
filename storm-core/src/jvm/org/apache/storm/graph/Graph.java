/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.graph;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

public class Graph {
  private static final TreeSet<Vertex> EMPTY_SET = new TreeSet<Vertex>();
  private LinkedHashMap<Vertex, TreeSet<Vertex>> adjList;
  private LinkedHashMap<String, Vertex> vertices;
  private LinkedHashMap<String, Edge> myEdges;
  private int numOfVertices;
  private int numOfEdges;

  public Graph() {
    adjList = new LinkedHashMap<Vertex, TreeSet<Vertex>>();
    vertices = new LinkedHashMap<String, Vertex>();
    myEdges = new LinkedHashMap<String, Edge>();
    numOfVertices = numOfEdges = 0;

  }

  /**
   * Add a new vertex name with no neighbors (if vertex does not yet exist)
   *
   * @param name vertex to be added
   */
  public Vertex addVertex(String name) {
    Vertex v;
    v = vertices.get(name);
    if (v == null) {
      v = new Vertex(name);
      numOfVertices += 1;
      v.setId(numOfVertices);
      vertices.put(name, v);
      adjList.put(v, new TreeSet<Vertex>());

    }
    return v;
  }

  /**
   * Returns the Vertex matching v
   *
   * @param name a String name of a Vertex that may be in
   *             this Graph
   * @return the Vertex with a name that matches v or null
   * if no such Vertex exists in this Graph
   */
  public Vertex getVertex(String name) {
    return vertices.get(name);
  }

  /**
   * Returns true iff v is in this Graph, false otherwise
   *
   * @param name a String name of a Vertex that may be in
   *             this Graph
   * @return true iff v is in this Graph
   */
  public boolean hasVertex(String name) {
    return vertices.containsKey(name);
  }

  /**
   * Is from-to, an edge in this Graph. The graph is
   * undirected so the order of from and to does not
   * matter.
   *
   * @param from the name of the first Vertex
   * @param to   the name of the second Vertex
   * @return true iff from-to exists in this Graph
   */
  public boolean hasEdge(String from, String to) {

    if (!hasVertex(from) || !hasVertex(to)) {
      return false;
    }
    String edgeName = from + ">" + to;
    return adjList.get(vertices.get(from)).contains(vertices.get(to));
  }

  public Edge getEdge(String from, String to) {
    String edgeName = from + ">" + to;
    return myEdges.get(edgeName);
  }

  /**
   * Add to to from's set of neighbors, and add from to to's
   * set of neighbors. Does not add an edge if another edge
   * already exists
   *
   * @param from the name of the first Vertex
   * @param to   the name of the second Vertex
   */
  public Edge addEdge(String from, String to) {
    Vertex src;
    Vertex dest;
    Edge edge = getEdge(from, to);
    if (edge != null) {
      return edge;
    }
    edge = new Edge(from, to);
    myEdges.put(edge.getName(), edge);
    numOfEdges += 1;

    src = getVertex(from);
    if (src == null) {
      src = addVertex(from);
    }
    dest = getVertex(to);
    if (dest == null) {
      dest = addVertex(to);
    }

    adjList.get(src).add(dest);
    adjList.get(dest).add(src);

    return edge;
  }

  /**
   * Return an iterator over the neighbors of Vertex named v
   *
   * @param name the String name of a Vertex
   * @return an Iterator over Vertices that are adjacent
   * to the Vertex named v, empty set if v is not in graph
   */
  public Iterable<Vertex> adjacentTo(String name) {
    if (!hasVertex(name)) {
      return EMPTY_SET;
    }
    return adjList.get(getVertex(name));
  }

  /**
   * Returns an Iterator over all Vertices in this Graph
   *
   * @return an Iterator over all Vertices in this Graph
   */
  public Iterable<Vertex> getVertices() {
    return vertices.values();
  }

  public int numVertices() {
    return numOfVertices;
  }

  public int numEdges() {
    return numOfEdges;
  }

  public String toString() {
    String str = "";
    for (Vertex v : this.getVertices()) {
      str += String.format("%s%s: ", v, v.getWeightsString());

      for (Vertex w : this.adjacentTo(v.getName())) {
        Edge e = this.getEdge(v.getName(), w.getName());
        String ew = "";
        if (e != null) {
          ew = e.getWeightString();
        }
        str += String.format("%s->%s%s", ew, w, w.getWeightsString());
      }
      str += "\n";
    }
    return str;
  }

  /**
   * Writes the input data required for METIS and Returns the data as a String
   */
  public String writeMetis(String fileName, boolean fmtVertexSize,
                           boolean fmtVertexWeight, boolean fmtEdgeWeight, int ncon) {
    String fmt = Integer.toString(fmtVertexSize ? 1 : 0)
        + Integer.toString(fmtVertexWeight ? 1 : 0)
        + Integer.toString(fmtEdgeWeight ? 1 : 0);


    String file = "output/" + fileName;
    StringBuilder line = new StringBuilder();

    try {
      FileWriter out = new FileWriter(file);

      //write header info (#v #e fmt ncon)
      line.append("%------header------\n")
          .append(this.numVertices() + " ")
          .append(this.numEdges() + " ")
          .append(fmt + " ")
          .append(ncon)
          .append("\n");
      for (Map.Entry<Vertex, TreeSet<Vertex>> row
          : adjList.entrySet()) {
        Vertex vertex = row.getKey();
        TreeSet<Vertex> neighbours = row.getValue();
        //write a comment to show the current(i'th) vertex name and it's weights
        //%--------#VName(#weights)--------
        line.append("%")
            .append("--------")
            .append(vertex.getName())
            .append(vertex.getWeightsString(ncon))
            .append("--------" + "\n");
        //write each line: the weights of current vertex and neighbours + edge weights(if exist)
        //#weights #neighbour1 #edgeWeight #neighbour2 #edgeWeight ...
        line.append(getNeighboursOf(vertex, neighbours, fmtEdgeWeight, ncon) + "\n")
            .append("%" + getNeighbourNamesOf(vertex, neighbours, fmtEdgeWeight) + "\n");
      }
      out.write(line.toString());
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return line.toString();
  }

  public String getNeighboursOf(Vertex currentVertex, TreeSet<Vertex> neighbours,
                                boolean fmtEdgeWeight, int ncon) {
    StringBuilder line = new StringBuilder();
    line.append(currentVertex.getWeightsString(ncon, false) + " ");
    Edge edge;
    for (Vertex vertex
        : neighbours) {
      edge = getEdge(currentVertex.getName(), vertex.getName());
      if (edge == null) {
        edge = getEdge(vertex.getName(), currentVertex.getName());
      }
      line.append(vertex.getId() + " ");
      if (fmtEdgeWeight) {
        line.append(edge.getWeightString(false) + " ");
      }
    }
    return line.toString().trim();
  }

  public String getNeighbourNamesOf(Vertex currentVertex, TreeSet<Vertex> neighbours,
                                    boolean fmtEdgeWeight) {
    StringBuilder line = new StringBuilder();
    Edge edge;
    for (Vertex vertex
        : neighbours) {
      edge = getEdge(currentVertex.getName(), vertex.getName());
      if (edge == null) {
        edge = getEdge(vertex.getName(), currentVertex.getName());
      }

      if (fmtEdgeWeight) {
        line.append(edge.getWeightString() + "->");
      }
      line.append(vertex.getName() + " ");

    }
    return line.toString().trim();
  }

  private String escapedVersion(String s) {
    return "\'" + s + "\'";

  }

  public void outputGDF(String fileName) {
    HashMap<Vertex, String> idToName = new HashMap<Vertex, String>();
    String file = "output/" + fileName;

    try {
      FileWriter out = new FileWriter(file);
      int count = 0;
      out.write("nodedef> name,label,style,distance INTEGER\n");
      // write vertices
      for (Vertex v : vertices.values()) {
        String id = "v" + count++;
        idToName.put(v, id);
        out.write(id + "," + escapedVersion(v.getName()));
        out.write(",6," + v.distance + "\n");
      }
      out.write("edgedef> node1,node2,color\n");
      // write edges
      for (Vertex v : vertices.values()) {
        for (Vertex w : adjList.get(v)) {
          if (v.compareTo(w) < 0) {
            out.write(idToName.get(v) + "," + idToName.get(w) + ",");
            if (v.predecessor == w || w.predecessor == v) {
              out.write("blue");
            } else {
              out.write("gray");
            }
            out.write("\n");
          }
        }
      }
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
