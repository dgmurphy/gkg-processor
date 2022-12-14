
import math
from lib.logging import logging
from lib.constants import *


def write_header(title, nodes, edges):

    header = []

    header.append("<!DOCTYPE html>")
    header.append("<html>")
    header.append(" <head>")
    header.append('  <meta charset="UTF-8">')
    header.append(f"  <title>{title}</title>")
    header.append('  <link rel="stylesheet" href="./styles.css" type="text/css">')
    header.append('  <script type="text/javascript" src="./vis-network.min.js"></script>')
    header.append( ' <script type="text/javascript">')
    header.append('     var nodes = null;')
    header.append('     var edges = null;')
    header.append('     var network = null;')
    header.append('     function draw() {')
    header.append(get_graph_data(nodes, edges))
    header.append('        var container = document.getElementById("mynetwork");')
    header.append('        var data = {nodes: nodes, edges: edges};')
    header.append("        var options = { nodes: {shape: 'dot'},")
    header.append("                        layout: {")
    header.append("                          randomSeed: undefined,")
    header.append("                          improvedLayout:false,")    
    header.append("                          clusterThreshold: 150,")
    header.append("                        },")
    header.append("                        physics: {")
    header.append("                          stabilization: {enabled:true, iterations:100},")
    header.append("                          barnesHut: {")
    header.append("                            gravitationalConstant: -20000")
    header.append("                          }")
    header.append("                        }")
    header.append("                     }")    
    header.append('        network = new vis.Network(container, data, options);')
    header.append('        network.once("beforeDrawing", function() {')
    header.append('            var loading =  document.getElementById("loading");')
    header.append('            loading.parentNode.removeChild(loading); }); ')
    header.append('         } </script>')
    header.append(" </head>")

    return header


def write_footer():

    footer = []
    footer.append(" </body>")
    footer.append("</html>")

    return footer


def get_graph_data(nodes, edges):

    nodes_str = "nodes = [\n"
    for node in nodes:
        nodes_str += node + ",\n"
    nodes_str += "];\n"
        

    edges_str = "edges = [\n"
    for edge in edges:
        edges_str += edge + ",\n"
    edges_str += "];\n"
        

    data = nodes_str + "\n" + edges_str
    return data


def main():

    TITLE = ORGS_GRAPH_TITLE
    OUTFILE = DATA_DIR + ORGS_GRAPH_FILE
 
    # load nodes and edges
    node_input_file = DATA_DIR + TOP_ORGS_FILE
    edge_input_file = DATA_DIR + TOP_ORGS_EDGE_IDS_FILE

    # nodes
    with open(node_input_file) as file:
        lines = list(file)

    #  get node scale
    max_node_size = 0
    for line in lines[1:]:
        line = line.strip()
        items = line.split(',')
        node_val_float = float(items[2].strip())
        if node_val_float > max_node_size:
            max_node_size = node_val_float

    print("max node size: " + str(max_node_size))


    nodes = []
    NODE_SCALE = 50
    for line in lines[1:]:
        line = line.strip()
        items = line.split(',')
        node_id = items[0].strip()
        node_value = items[2].strip()
        node_value = round( (float(node_value) / max_node_size) * NODE_SCALE)
        node_label = items[1].strip()
        row = ("{id: " + node_id + ", value: " +
            str(node_value) + ", label: " + "'" + node_label + "'}")
        nodes.append(row)

    #print(str(nodes))

    # edges
    with open(edge_input_file) as file:
        lines = list(file)


    #  get edge scale
    max_edge_size = 0
    for line in lines[1:]:
        line = line.strip()
        items = line.split(',')
        edge_val_float = float(items[2].strip())
        if edge_val_float > max_edge_size:
            max_edge_size = edge_val_float

    print("max edge size: " + str(max_edge_size))

    
    edges = []
    EDGE_SCALE = 10
    for line in lines[1:]:
        line = line.strip()
        items = line.split(',')
        node_from = items[0].strip()
        node_to = items[1].strip()
        edge_value = items[2].strip()
        edge_value = math.ceil( (float(edge_value) / max_edge_size) * EDGE_SCALE)
        row = ("{from: " + node_from + ", to: " +
            node_to + ", value: " + str(edge_value) + ", title: " + 
            "'" +  str(edge_value) + "'}")
        edges.append(row)
   
    #print(str(edges))



    # HTML Start
    html = write_header(TITLE, nodes, edges)
    html.append('<body onload="draw()">')

    html.append(f'<h1> {TITLE}</h1>')
    html.append('<p><a href="./all_orgs_edges.csv">Full Edge List</a></p>')
    html.append('<p><a href="./all_orgs.csv">Full Nodes List</a></p>')
    html.append('<div id="loading">')
    html.append('<div style="margin: 40px 0px 0px 210px; font-weight:bold; color:blue">Building graph...</div>')
    html.append('<div><img src="./loading.gif"/></div></div>')
    html.append('<div id="mynetwork"></div>')

    # CLOSING HTML
    footer = write_footer()
    for line in footer:
        html.append(line)

    logging.info("writing " + OUTFILE)
    with open(OUTFILE, 'w') as f:
        for line in html:
            f.writelines(line + "\n")  

if __name__ == '__main__':
    main()
    print("DONE\n")