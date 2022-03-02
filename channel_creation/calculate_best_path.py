def calc_mean_sdist(data):
    
    
    import graph_tool.all as gt
    my_node_int = data[0]
    new_channel_weight = data[1]
    target_node = data[2]

    to_pnd = {}

    g = gt.load_graph(f'{base_path}/graph_test.gt')
    weight = g.ep['weight']


    e = g.add_edge(my_node_int, target_node)

    weight[e] = int(new_channel_weight*1000)

    dist = gt.shortest_distance(g,weights=weight)
    ave_path_length = sum([sum(i) for i in dist])/(g.num_vertices()**2-g.num_vertices())

    dist = gt.betweenness(g,weight=weight)
    betweenness = dist[0][my_node_int]


    #pagerank = gt.pagerank(g,weight=weight)[my_node_int]
    closeness = gt.closeness(g,weight=weight)[my_node_int]

    #hits = gt.hits(g,weight=weight)

    to_pnd['to_node_int'] = target_node
    to_pnd['betweenness'] = betweenness
    to_pnd['ave_path_length'] = ave_path_length
    #to_pnd['pagerank'] = pagerank
    to_pnd['closeness'] = closeness


    to_pnd['hits_aut_cent'] = 0#hits[1][my_node_int]
    to_pnd['hits_hub_cent'] = 0#hits[2][my_node_int]
    
    
    return to_pnd



# Load to_send and test
to_send = pickle.load(open(f"{base_path}/to_send.pkl","rb"))
g = gt.load_graph(f"{base_path}/graph_test.gt")
data = pickle.load(open(f"{base_path}/data.pkl",'rb'))

#checking betweeness without adding channels
weight = g.ep['weight']
dist = gt.betweenness(g,weight=weight)
betweenness = dist[0][to_send[0][0]]
print(betweenness)

new_c_id = data['chans'].sort_values("spendable_msat").iloc[-1]['id']
data['name_to_int'][new_c_id]
print(f"Now adding channel to {data['id_to_alias'][new_c_id]}")


# data['chans']

output_a = calc_mean_sdist([to_send[0][0],to_send[0][1],data['name_to_int'][new_c_id]])

output_b = calc_mean_sdist(to_send[10])

print(f"betweenness before channel = {betweenness}")
print(f"betweenness after channel  = {output_a['betweenness']}")
print("Should stay extermley similar")

print(f"Adding to a different node = {output_b['betweenness']}")
print("Should show a bigger difference")



rc = ipp.Client()
rc[:].push(dict(base_path=base_path))
dview = rc.load_balanced_view()
ar = dview.map_async(calc_mean_sdist, to_send)


all_out =[]
j = 1 
for i in tqdm(ar,total=len(list(g.vertices()))):
    all_out.append(i)
    if j%300==0:
        df_out = pd.DataFrame(all_out)
        df_out.to_pickle(f"{base_path}/df_out.pkl")
    j+=1
df_out.to_pickle(f"{base_path}/df_out.pkl")
