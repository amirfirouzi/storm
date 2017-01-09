// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.storm.graph;

import java.io.*;

/**
 * Created by amir on 11/21/16.
 */
public final class GraphSerializer {

  private GraphSerializer() {
    //not called
  }

  public static void serializeGraph(Graph graph) {
    try {
      FileOutputStream fos = new FileOutputStream("output/graphdata.ser");
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(graph);
      oos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Graph deSerializeGraph() throws Exception {
    Graph graph = null;
    FileInputStream fis = new FileInputStream("output/graphdata.ser");
    ObjectInputStream ois = new ObjectInputStream(fis);
    graph = (Graph) ois.readObject();
    ois.close();
    return graph;
  }
}
