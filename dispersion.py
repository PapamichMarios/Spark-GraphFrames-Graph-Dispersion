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
from pyspark.sql.functions import explode, explode_outer
from itertools import combinations
import sys

maxPrintSize = 500


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


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
vertices = edges.groupBy("src").agg(F.collect_set("dst").alias("neighbours_list"))
vertices = vertices.withColumnRenamed("src", "id")


# add neighbours column having [node_id, neighbours_list]
def new_neighbours(id, neighbours):
    return {
        "id": id, 
        "neighbours": neighbours
    }


neighbours_type = types.StructType(
    [types.StructField("id", types.StringType()), types.StructField("neighbours", types.ArrayType(types.StringType()))])
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
# aggregates.show()

def common_neighbours(node_neighbours, messagers_neighbours):
    common_list = []
    for neighbours in messagers_neighbours:
        common_list.append({"id": neighbours.id, "neighbours": intersection(neighbours.neighbours, node_neighbours)})

    return common_list


neighbour_list_type = types.ArrayType(neighbours_type)
common_neighbours_udf = F.udf(common_neighbours, neighbour_list_type)

common = aggregates.withColumn("common_neighbours",
                               common_neighbours_udf(aggregates["neighbours_list"], aggregates["agg"])).drop("agg").drop("neighbours_list")
common.show()

# dispersion
print("Dispersion")


def calculate_dispersion(common_neighbours):
    hashmap = {}
    for neighbours in common_neighbours:
        hashmap[neighbours['id']] = neighbours['neighbours']

    dispersion_list = []
    for common_neighbour in common_neighbours:

        dispersion = 0
        for s,t in combinations(common_neighbour['neighbours'], 2):

            # if they are connected with an edge
            if s in hashmap[t]: continue

            # they share neighbours other than u and v
            common = intersection(hashmap[s], hashmap[t])
            if common_neighbour['id'] in common: common.remove(common_neighbour['id'])
            if len(common) > 0: continue

            dispersion += 1

        dispersion_list.append({
            "id": common_neighbour['id'],
            "dispersion": dispersion
        })

    maximum = max(dispersion_list, key=lambda x:x['dispersion'])
    return maximum

calculate_dispersion_type = types.StructType([types.StructField("id", types.StringType()), types.StructField("dispersion", types.IntegerType())])
calculate_dispersion_udf = F.udf(calculate_dispersion, calculate_dispersion_type)

dispersion = common.withColumn("dispersion", calculate_dispersion_udf(common["common_neighbours"])).drop("common_neighbours")
dispersion.show()

# final graph with dispersion on edges
print("Final Graph:")
dispersion = dispersion.withColumnRenamed("node", "src")
cached_vertices = AM.getCachedDataFrame(dispersion)
graph = GraphFrame(cached_vertices, graph.edges)
graph.vertices.show(maxPrintSize)
graph.edges.show()
