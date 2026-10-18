========================================================================
LARGE-SCALE SOCIAL NETWORK STRUCTURE ANALYSIS
Distributed Community Detection & Link Prediction
========================================================================

PROJECT STRUCTURE:
-----------------
1_Dataset/
    - pokec_3M.txt: Benchmark dataset (3,000,000 edges, 663,230 vertices)
    - dataset_info.txt: Dataset description and SNAP reference

2_Hadoop_MapReduce/
    - CommunityDetection.java: MapReduce Label Propagation Algorithm
    - CommunityDetection.jar: Pre-compiled executable JAR for Hadoop/YARN
    - commands.txt: Cluster execution commands

3_Apache_Spark/
    - src/main/java/SocialNetworkAnalysisSpark.java: Apache Spark Java RDD implementation
    - pom.xml: Maven configuration
    - run_spark.bat: Batch runner for Spark

4_Spark_GraphX/
    - src/main/scala/SocialNetworkAnalysis.scala: Apache Spark GraphX Scala implementation
    - SocialNetworkAnalysis.scala: Standalone Scala script for spark-shell
    - pom.xml: Maven configuration
    - run_graphx.bat: Batch runner for GraphX

Project_Report.md:
    - Detailed academic report with performance comparisons and results
