"""
The goal is to use aggregateMessages in order to find the dispersion for pairs of nodes

Run with: ~/spark-2.3.4-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11  /vagrant/dispersion.py

"""

from pyspark.sql import SparkSession
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions as sqlfunctions, types
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from itertools import combinations
import sys

maxPrintSize = 500

# set up a spark session
spark = SparkSession.builder.appName('graph-dispersion').getOrCreate()

# read csv input
vertices = spark.read.csv("/vagrant/input_graph/star-wars_vertices.csv", header=True)
edges = spark.read.csv("/vagrant/input_graph/star-wars_edges.csv", header=True)
print("Original Graph:")
graph = GraphFrame(vertices, edges)
graph.vertices.show()
graph.edges.show()

# get reverse because we are refering to undirected graphs
reverse_edges = edges.selectExpr("dst as src", "src as dst")
edges = edges.union(reverse_edges)

# add neighbours_list column having all the neighbours ids
vertices = edges.groupBy("src").agg(F.collect_list("dst").alias("neighbours_list"))
vertices = vertices.withColumnRenamed("src", "id")

# add neighbours column having [node_id, neighbours_list]
def new_neighbours(id, neighbours):
    return {"id": id, "neighbours": neighbours}
neighbours_type = types.StructType([types.StructField("id", types.StringType()), types.StructField("neighbours", types.ArrayType(types.StringType()))])
new_neighbours_udf = F.udf(new_neighbours, neighbours_type)

vertices = vertices.withColumn("neighbours", new_neighbours_udf(vertices["id"], vertices["neighbours_list"]))

# construct the graph
graph = GraphFrame(vertices, edges)
print("Graph after tweaks:")
graph.vertices.show()
graph.edges.show()

# send neighbours list to neighbours
aggregates = graph.aggregateMessages(F.collect_set(AM.msg).alias("agg"), sendToDst=AM.src["neighbours"])
print("Using aggregateMessages:")
aggregates.show()

# find common neighbours
print("Finding common neighbours:")
aggregates = aggregates.join(graph.vertices, on="id").drop("neighbours")
aggregates = aggregates.select(aggregates["id"], aggregates["neighbours_list"].alias("node_neighbours"), explode(aggregates["agg"]).alias("neighbours"))
# aggregates.show(maxPrintSize, truncate=False)

def neighbour_id(neighbours):
    return neighbours.id
neighbour_id_type = types.StringType()
neighbour_id_udf = F.udf(neighbour_id, neighbour_id_type)

def neighbour_list(neighbours):
    return neighbours.neighbours
neighbour_list_type = types.ArrayType(types.StringType())
neighbour_list_udf = F.udf(neighbour_list, neighbour_list_type)

aggregates = aggregates.withColumn("neighbour", neighbour_id_udf(aggregates["neighbours"])).withColumn("messagers_neighbours", neighbour_list_udf(aggregates["neighbours"])).drop("neighbours")
# aggregates.show(maxPrintSize, truncate=False)

def common_neighbours(node_neighbours, messagers_neighbours):
    common_neighbours_list = []
    for neighbour in messagers_neighbours:
        if neighbour in node_neighbours:
            common_neighbours_list.append(neighbour)

    return common_neighbours_list
neighbour_list_type = types.ArrayType(types.StringType())
common_neighbours_udf = F.udf(common_neighbours, neighbour_list_type)

common = aggregates.withColumn("common_neighbours", common_neighbours_udf(aggregates["node_neighbours"], aggregates["messagers_neighbours"])).drop("messagers_neighbours").drop("node_neighbours")
common.show()

# dispersion
print("Calculating dispersion...")
dispersion = common.select(common["id"].alias("node"), common["neighbour"], explode(common["common_neighbours"]).alias("id"))
# dispersion.show(maxPrintSize, truncate=False)

dispersion = dispersion.join(graph.vertices, on="id").drop("neighbours").withColumnRenamed("id", "common_neighbours").withColumnRenamed("neighbours_list", "neighbours_of_common_neighbours")
# dispersion.show(maxPrintSize, truncate=False)

dispersion = dispersion.groupBy("node", "neighbour").agg(F.collect_list("common_neighbours").alias("common_neighbours"), F.collect_list("neighbours_of_common_neighbours").alias("neighbours_of_common_neighbours"))
# dispersion.show(maxPrintSize, truncate=False)

def calculate_dispersion(node, neighbour, common_neighbours, neighbours_of_common_neighbours):
    hashmap = {}
    for i in range(0, len(common_neighbours)):
        hashmap[common_neighbours[i]] = neighbours_of_common_neighbours[i]

    dispersion = 0 
    for x,y in combinations(common_neighbours, 2):

        # they are connected with an edge
        if x in hashmap[y]:
            continue

        # they share neighbours othen than u and v
        def intersection(lst1, lst2): 
            lst3 = [value for value in lst1 if value in lst2] 
            return lst3 

        common = intersection(hashmap[x], hashmap[y])
        if node in common:
            common.remove(node)

        if neighbour in common:
            common.remove(neighbour)

        if len(common) > 0:
            continue

        dispersion += 1
    return dispersion
calculate_dispersion_type = types.IntegerType()
calculate_dispersion_udf = F.udf(calculate_dispersion, calculate_dispersion_type)

dispersion = dispersion.withColumn("dispersion", calculate_dispersion_udf(dispersion["node"], dispersion["neighbour"], dispersion["common_neighbours"], dispersion["neighbours_of_common_neighbours"])).drop("common_neighbours").drop("neighbours_of_common_neighbours")
# dispersion.show()

# final graph with dispersion on edges
print("Final Graph:")
dispersion = dispersion.withColumnRenamed("node", "src").withColumnRenamed("neighbour", "dst")
cached_edges = AM.getCachedDataFrame(dispersion)
graph = GraphFrame(graph.vertices, cached_edges)
graph.vertices.show(maxPrintSize)
graph.edges.show(maxPrintSize)