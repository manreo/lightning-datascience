from lightning import LightningRpc 
import pandas as pd
import networkx as nx
from tqdm.auto import tqdm
import pickle
import time 
from collections import defaultdict
l1 = LightningRpc("????") 
channels = l1.listchannels()
nodes =  l1.listnodes()
peers = l1.listpeers()['peers']


id_to_alias = {}
for node in nodes['nodes']:
    if 'alias' in node:
        id_to_alias[node['nodeid']] = node['alias']
    else:
        id_to_alias[node['nodeid']] = 'un-def'
        


to_df = {'source':[], 'destination':[], 'amount':[],
        'last_update':[], 'active':[], 'base_fee':[], 'fee_per_million':[]}
        

df = pd.DataFrame(channels['channels'])

#weight will be set as the fee to transfer 50,000 sats
transfer_amount = 30*1000

#the minimum sats a node needs to have to be considered
nodes_min_sats = 1000000
nodes_min_channels = 5

my_node = '????'
my_base_fee = 1 #milsat 
my_per_millionth =  1 #


new_channel_weight = transfer_amount*(my_per_millionth/1000000)+my_base_fee/1000

df['weight'] = df['base_fee_millisatoshi']/1000 + (df['fee_per_millionth']/1000000)*transfer_amount




## find all nodes that I am connected that are disabled by checking if >50% of there channles are off
print("the following channels were removed as they are not active!")
to_delete = []
for _,c in df.query('active == False  and (source==@my_node or destination==@my_node)').iterrows():
    if c["destination"]==my_node:
        continue
    # active channels from this unactive channel:
    active_chans = len(df.query('active == True  and (source==@c["destination"] or destination==@c["destination"])') )
    nonactive_chans = len(df.query('active == False  and (source==@c["destination"] or destination==@c["destination"])'))
    print(active_chans,nonactive_chans)
    if active_chans/(nonactive_chans+active_chans)<0.5:
        to_delete.append(c["destination"])
        print(f"DELETE - from {id_to_alias[c['source']]} to {id_to_alias[c['destination']]}")
    else:
        print(f"DONT DELETE - from {id_to_alias[c['source']]} to {id_to_alias[c['destination']]}")


#filter diabled channels, beside those that with my node
df_t = df.query('active==True or ((source==@my_node or destination==@my_node) and (source not in @to_delete and destination not in @to_delete))')


#remove nodes with low capacity or node with small number of channels
remove_nodes_amount = list(df_t.groupby("source")['satoshis'].sum().reset_index().query("satoshis<@nodes_min_sats")['source'])
remove_nodes_channels = list(df_t.groupby("source").count().query("short_channel_id<@nodes_min_channels").index)

df_t = df_t.query('source not in @remove_nodes_amount and destination not in @remove_nodes_amount').copy()
df_t = df_t.query('source not in @remove_nodes_channels and destination not in @remove_nodes_channels').copy()

#remove channels with less than 1/5 of the transfer amount
df_t = df_t.query('satoshis>@transfer_amount/5').copy()

#remove channels with fee of 10%
df_t = df_t.query("weight<(@transfer_amount*0.1) or source==@my_node  or destination==@my_node").copy()

#remove channels that do not exists twice (from both directions, open a ticket!)
both_sides = df_t[['short_channel_id']].value_counts().reset_index().rename(columns={0:'count'}).query("count>1")['short_channel_id']
df_t = df_t.query('short_channel_id in @both_sides')
print(df_t.query("source==@my_node and destination=='03c5528c628681aa17ab9e117aa3ee6f06c750dfb17df758ecabcd68f1567ad8c1'"))

all_peers_ids = [peer['id'] for peer in peers]
print(set(list(df_t.query("source==@my_node").destination)) - set(all_peers_ids))

# remove my channels that do not have enough in/out
for peer in peers:
    if len(df_t.query("source==@my_node and destination==@peer['id']"))==0:
        if peer['id'] not in id_to_alias:
            id_to_alias[peer['id']] = 'un-def1'
        print(f"path to {id_to_alias[peer['id']]} not exists"*5)
        print(peer['id'])
        print("It might be a very small node... Or high fee")
        tm = len(df.query("source==@my_node and destination==@peer['id']"))>0
        print(f"Exist in orginal df = {tm}")
        continue
    
    # if spendable is lower than amount, remove connection
    # else make sure the fee is new_channel_weight!
    if peer['channels'][0]['spendable_msat'].to_satoshi()<transfer_amount/1.3:
        ind = df_t.query("source==@my_node and destination==@peer['id']").index[0]
        print(f"delete out =  {id_to_alias[peer['id']]}")
        df_t = df_t.drop(ind)
    else:
        df_t.loc[df_t.query("source==@my_node and destination==@peer['id']").index,'weight'] = new_channel_weight
        print(f"Keep out =  {id_to_alias[peer['id']]}")

    #if I can not get the transfer amount remove the cahnnel in my direction
    if peer['channels'][0]['receivable_msat'].to_satoshi()<transfer_amount/1.3:
        ind = df_t.query("destination==@my_node and source==@peer['id']").index[0]
        print(f"delete in = {id_to_alias[peer['id']]} ")
        df_t = df_t.drop(ind)
    else:
        print(f"Keep in =  {id_to_alias[peer['id']]}")

#keep only the largest strongly connected component
G = nx.from_pandas_edgelist(df_t, source='source',target='destination',edge_attr=['weight'],create_using=nx.DiGraph)
larget_subgraph_nodes = max(nx.weakly_connected_components(G))
df_t = df_t.query('source in @larget_subgraph_nodes and destination in @larget_subgraph_nodes').copy()


#set naming
all_nodes = set(list(df_t.source) + list(df_t.destination))
int_to_name = {i:j for i,j in enumerate(all_nodes)}
name_to_int = {int_to_name[j]:j for j in int_to_name}

df_t['source_int'] = df_t['source'].apply(lambda x:name_to_int[x])
df_t['destination_int'] = df_t['destination'].apply(lambda x:name_to_int[x])

#so I can use ints!
df_t['weight_int']= df_t['weight']*1000



import graph_tool.all as gt
g = gt.Graph(directed=True)
weight = g.new_edge_property("int64_t")
g.ep['weight'] = weight

#create graph
for _,row in df_t.iterrows():
    e = g.add_edge(row['source_int'], row['destination_int'])
    weight[e] = int(row['weight_int'])


      
        

def calc_mean_sdist(data):
    import graph_tool.all as gt
    my_node_int = data[0]
    new_channel_weight = data[1]
    target_node = data[2]
    
    to_pnd = {'to_node_int':[],'avg':[]}
    
    g = gt.load_graph('graph_test.gt')
    weight = g.ep['weight']   
    
    e = g.add_edge(my_node_int, target_node)
    
    weight[e] = int(new_channel_weight*1000)
    
    dist = gt.shortest_distance(g,weights=weight)
    save_path_length = sum([sum(i) for i in dist])/(g.num_vertices()**2-g.num_vertices())
    
    
    to_pnd['to_node_int'] = target_node
    to_pnd['avg'] = save_path_length
    return to_pnd
 
#@save also all the channels that I have
#@make sure that all out paths from me have the same wight as the new added channel!
to_send = [[name_to_int[my_node],new_channel_weight,node] for node in range(len(list(g.vertices())))]
 
g.save('/home/machine/graph_test.gt')

pickle.dump(to_send,open("??/to_send.pkl",'wb'))

#Now also save the channels that I have
dfc = pd.DataFrame([i['channels'][0] for i in peers if len(i['channels'])])
dfp = pd.DataFrame([i for i in peers if len(i['channels'])])
dfb = pd.concat([dfp,dfc],axis=1)
pickle.dump({'my_node':my_node,'df_t':df_t, 'name_to_int':name_to_int, 'int_to_name':int_to_name, 'id_to_alias':id_to_alias,'chans':dfb}, open("??/data.pkl",'wb'))
