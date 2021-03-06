/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.graph;

import org.apache.storm.scheduler.ExecutorDetails;

public class Vertex implements Comparable<Vertex> {
    /**
     * label for Vertex
     */
    private String name;
    private int id;
    private Resource weights;
    private ExecutorDetails executor;

    public Vertex(String v) {
        name = v;
        weights = null;
    }

    public Vertex(String v, ExecutorDetails executor) {
        name = v;
        weights = null;
        this.executor = executor;
    }

    public Vertex(String v, ExecutorDetails executor, Resource weights) {
        name = v;
        this.weights = weights;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public ExecutorDetails getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorDetails executor) {
        this.executor = executor;
    }

    /**
     * The name of the Vertex is assumed to be unique, so it
     * is used as a HashCode
     *
     * @see Object#hashCode()
     */
    public int hashCode() {
        return name.hashCode();
    }

    public void addWeights(Resource vertexWeights) {
        this.weights = vertexWeights;
    }

    public Resource getWeights() {
        return this.weights;
    }

    public String getWeightsString() {
        String vw = "";
        if (weights != null) {
            vw += "CPU: " + this.weights.getCpu()
                    + "\n "
                    + "MEM: " + this.weights.getMemory();
        }
        return vw;
    }

    public String toString() {
        return name + "(" + executor + ")";
    }

    /**
     * Compare on the basis of distance from source first and
     * then lexicographically
     */
    public int compareTo(Vertex other) {
        return id;
    }
}

