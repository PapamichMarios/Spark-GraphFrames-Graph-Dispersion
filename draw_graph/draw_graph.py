import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt

# star wars
star_wars = "star-wars.csv"
star_wars_text = pd.read_csv(star_wars)

df = pd.read_csv(star_wars)

# star wars
df = df[['Source', 'Target']]
df.drop_duplicates(subset=['Source', 'Target'], inplace=True)

G = nx.from_pandas_edgelist(df, source='Source', target='Target')
pos = nx.spring_layout(G)
nx.draw(G, pos=pos, alpha=0.8)

pos_attrs = {}
for node, coords in pos.items():
    pos_attrs[node] = (coords[0], coords[1] + 0.05)

nx.draw_networkx_labels(G, pos=pos_attrs, font_size=14)
plt.show()
