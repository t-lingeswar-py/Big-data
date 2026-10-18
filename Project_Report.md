# Course Project Report: Large-Scale Social Network Structure Analysis
## Distributed Community Detection and Link Prediction across Hadoop MapReduce, Apache Spark, and Spark GraphX

---

### Executive Summary
In large-scale online social networks, analyzing graph topology is critical for friend recommendation engines, community mining, and structural security. However, traditional single-machine network analysis tools (such as NetworkX or Gephi) crash or exhaust physical memory when faced with tens of millions of vertices and edges. 

This project implements and benchmarks a production-grade Big Data pipeline analyzing the **Stanford SNAP Pokec Social Network** (over 30 million relationships across 1.63 million users), evaluated across three distinct distributed paradigms:
1. **Hadoop Distributed File System (HDFS)** for distributed fault-tolerant block storage.
2. **Hadoop MapReduce (Java)** for iterative distributed Community Detection (Label Propagation).
3. **Apache Spark (Java RDD API)** for in-memory graph construction, distributed community detection, and common neighbors link prediction with live DAG tracking via the Spark Web UI.
4. **Apache Spark GraphX (Scala)** utilizing native graph-parallel abstractions (`Graph[VD, ED]`), Pregel-based iterative primitives (`lib.LabelPropagation.run`), and degree-capped common neighbors heuristics.

---

## 1. Problem Statement & Objectives
Online social networks exhibit power-law degree distributions, dense community clusters, and celebrity "hubs". Analyzing these graphs presents three key computational challenges:
1. **Scalability:** Storing and shuffling millions of adjacency lists across distributed workers without bottlenecking I/O.
2. **Community Detection:** Partitioning hundreds of thousands of users into cohesive structural communities where members interact more densely internally than externally.
3. **Link Prediction:** Recommending future relationships between unlinked users based on shared neighborhood topology (Common Neighbors heuristic), while overcoming the combinatorial explosion caused by high-degree hub nodes.

---

## 2. Dataset Description
The project utilizes the **Pokec Social Network** dataset from the **Stanford Large Network Dataset Collection (SNAP)**. Pokec is the premier social networking service in Slovakia:
* **Graph Type:** Directed Multigraph
* **Full Dataset:** 1,632,803 Vertices (Users) | 30,622,564 Directed Edges (Relationships)
* **Evaluated Benchmark Partition:** 663,230 Unique Vertices | 3,000,000 Directed Edges (`pokec_3M.txt`)
* **File Format:** Tab-separated raw edge list (`Source_Node_ID \t Destination_Node_ID`)

---

## 3. System Architecture & Technologies Used

```
                       +-----------------------------------+
                       |    Stanford SNAP Pokec Dataset    |
                       |       (30.6M / 3.0M Edges)        |
                       +-----------------+-----------------+
                                         |
                                         v
                       +-----------------------------------+
                       |    HDFS Storage (Distributed)     |
                       |       /user/data/pokec_3M.txt     |
                       +--------+-----------------+--------+
                                |                 |
          +---------------------+                 +---------------------+
          |                                                             |
          v                                                             v
+------------------------+                                    +------------------------+
| Hadoop MapReduce (Java)|                                    |  Apache Spark Engine   |
|   Community Detection  |                                    | (Web UI on Port 4040)  |
|  (Label Propagation)   |                                    +----+--------------+----+
+------------------------+                                         |              |
                                                                   v              v
                                                       +---------------+  +---------------+
                                                       | Spark Java    |  | Spark GraphX  |
                                                       | (RDD Engine)  |  | (Scala Graph) |
                                                       +---------------+  +---------------+
```

* **Hadoop HDFS (v3.3.6):** Stores graph partitions across 128 MB blocks with continuous health inspection via `hdfs fsck`.
* **Hadoop MapReduce (Java):** Custom Mapper and Reducer classes implementing iterative label broadcasting over YARN.
* **Apache Spark (v3.5.1):** High-performance distributed engine leveraging in-memory RDD caching and catalyst DAG execution.
* **Apache Spark GraphX (Scala 2.12):** Purpose-built graph-parallel framework offering native `Graph.fromEdges` abstraction and built-in optimized libraries.
* **Spark Web UI & DAG Visualizer (Port 4040):** Real-time cluster monitoring tracking stage decomposition, shuffle read/write sizes, and task execution skew.

---

## 4. Algorithms & Implementation Details

### 4.1 Community Detection: Label Propagation Algorithm (LPA)
* **Mathematical Intuition:** Given graph $G = (V, E)$, each vertex $v \in V$ is initialized with a distinct community label $C_v^{(0)} = v$. At each iterative superstep $t$:
  $$C_v^{(t)} = \arg\max_{c} \sum_{u \in N(v)} \mathbb{I}(C_u^{(t-1)} = c)$$
  Every vertex updates its community label to the majority label among its immediate neighbors $N(v)$.
* **MapReduce Implementation:** 
  - **Mapper:** For every directed edge $(u, v)$, emits key-value pairs representing mutual label votes $(u \to v)$ and $(v \to u)$.
  - **Reducer:** Aggregates incoming candidate labels for each user, computes frequency counts in a local HashMap, and emits the dominant community ID.
* **Spark GraphX Implementation:** Uses GraphX's built-in `lib.LabelPropagation.run(graph, maxIterations = 5)` executing across distributed resilient graph partitions with Pregel messaging.

### 4.2 Link Prediction: Common Neighbors Heuristic
* **Mathematical Intuition:** The Common Neighbors heuristic calculates the overlap of friends between two unconnected users $u$ and $v$:
  $$Score(u, v) = |N(u) \cap N(v)|$$
  Higher scores indicate a higher probability of mutual real-world friendship and future connection.
* **Combinatorial Optimization (Hub Protection):**
  In real-world networks like Pokec, celebrity nodes (hubs) have thousands of followers. Generating all pairs $\binom{k}{2}$ from a node with degree $k = 10,000$ generates $49,995,000$ candidate pairs for a single hub, leading to Java heap OutOfMemory (OOM) errors.
  To make large-scale execution deterministic and memory-safe, candidate neighbor expansions are capped at $K=30$ per hub, followed by distributed symmetrical pair grouping (`a < c`) and RDD key aggregation (`reduceByKey(_ + _)`).

---

## 5. Experimental Results & Benchmark Analysis

All benchmarks were executed on the 3,000,000 edge Pokec partition containing **663,230 unique users**.

### 5.1 Performance Comparison Table

| Metric / Stage | Hadoop MapReduce (Java) | Apache Spark (Java RDD) | Apache Spark GraphX (Scala) |
| :--- | :---: | :---: | :---: |
| **Language & Environment** | Java 8 / YARN Cluster | Java 21 / Spark 3.5.1 | Scala 2.12 / Spark 3.5.1 |
| **Storage Layer** | HDFS (`/user/data/`) | Local Memory + Disk | Local Memory + Disk |
| **Graph Construction Time** | N/A (Direct Disk Scan) | 17.20 seconds | **4.33 seconds** |
| **Community Detection (LPA)**| 38.00 seconds (1 iteration) | **4.94 seconds** (1 pass) | 403.34 seconds (5 supersteps) |
| **Link Prediction Time** | Not feasible in 1 MR pass | 78.10 seconds | 82.47 seconds |
| **Total Pipeline Runtime** | > 120 seconds | **100.35 seconds** | 521.43 seconds |
| **Execution Monitoring** | YARN Resource Manager UI | Spark Web UI (Port 4040) | Spark Web UI (Port 4040) |

### 5.2 Community Detection Sample Output
Both implementations successfully partitioned 663,230 users into modular community clusters:

```
Sample of Identified Communities (User ID -> Assigned Community ID):
  User ID: 612052   ===> Assigned Community: 51895   
  User ID: 354796   ===> Assigned Community: 26981   
  User ID: 1022816  ===> Assigned Community: 45707   
  User ID: 129434   ===> Assigned Community: 36841   
  User ID: 194402   ===> Assigned Community: 8413    
  User ID: 182316   ===> Assigned Community: 5867    
  User ID: 199516   ===> Assigned Community: 9360    
  User ID: 332918   ===> Assigned Community: 46829   
  User ID: 627804   ===> Assigned Community: 49198   
  User ID: 693158   ===> Assigned Community: 62792   
```

### 5.3 Link Prediction Results (Top Recommendations)
The distributed Common Neighbors heuristic ranked user pairs with the highest structural affinity:

```
Top 10 Predicted Links (User A <---> User B) -> Shared Mutual Friends:
  1. Pair: User 1891     <---> User 5867     | Mutual Connections: 4,595
  2. Pair: User 5867     <---> User 6228     | Mutual Connections: 2,509
  3. Pair: User 1891     <---> User 6228     | Mutual Connections: 2,277
  4. Pair: User 5867     <---> User 25994    | Mutual Connections: 1,368
  5. Pair: User 1891     <---> User 25994    | Mutual Connections: 1,282
  6. Pair: User 1830     <---> User 5867     | Mutual Connections: 1,198
  7. Pair: User 1830     <---> User 1891     | Mutual Connections: 1,123
  8. Pair: User 1830     <---> User 6228     | Mutual Connections: 744
  9. Pair: User 6228     <---> User 25994    | Mutual Connections: 704
 10. Pair: User 20372    <---> User 44671    | Mutual Connections: 483
```
*Key Finding:* Users **1891** and **5867** share **4,595 mutual connections**, demonstrating immense probability of real-world affiliation despite having no pre-existing direct edge in the raw dataset.

---

## 6. Spark Web UI & DAG Visualizer Insights (Port 4040)
While Spark executed locally, the Spark Web UI was inspected via `http://localhost:4040`:
1. **DAG Graph Stages:** Spark decomposed the RDD transformations into discrete stages separated by shuffle boundaries (`groupByKey` and `reduceByKey`).
2. **Shuffle Efficiency:** Vertex distinct counts and pair-generation stages exhibited balanced partitions across all available hardware CPU threads.
3. **In-Memory Caching:** RDD caching (`edges.cache()` and `graph.cache()`) prevented duplicate scans of the 3-Million row edge dataset between the community detection and link prediction phases.

---

## 7. Key Takeaways & Conclusion
1. **Framework Specialization:** MapReduce excels at fault-tolerant sequential batch scans, but is constrained for multi-pass graph algorithms by intermediate disk serialization between job steps.
2. **Spark In-Memory Performance:** Apache Spark achieved a **7.7x speedup** over MapReduce for iterative label broadcasting through resilient distributed memory caching.
3. **GraphX Abstraction:** Spark GraphX provides a mathematically clean vertex-edge paradigm that avoids manual RDD join management, with native support for industrial-strength graph algorithms.

---

## 8. References
1. Leskovec, J., & Krevl, A. (2014). *SNAP Datasets: Stanford Large Network Dataset Collection*.
2. Zaharia, M. et al. (2012). *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*. USENIX NSDI.
3. Gonzalez, J. E. et al. (2014). *GraphX: Graph Processing in a Distributed Dataflow Framework*. USENIX OSDI.
4. Raghavan, U. N., Albert, R., & Kumara, S. (2007). *Near linear time algorithm to detect community structures in large-scale networks*. Physical Review E.
