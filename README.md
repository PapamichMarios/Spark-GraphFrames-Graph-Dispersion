# Project
Using Spark and GraphFrames package to find the dispersion for each node in a graph G=(V, E).

# Tools Used
- Networkx
- Spark 2.3.4
- GraphFrames 0.8.0


# Installation & Execution
```
vagrant up
vagrant ssh
~/spark-2.3.4-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11  /vagrant/dispersion.py
vagrant destroy
```

# Sample Execution
- Sample execution for a Star Wars graph within ```input_graph```
- Draw of the input graph within ```draw_graph```
