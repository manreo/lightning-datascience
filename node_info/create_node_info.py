from datetime import datetime
from pathlib import Path

import bokeh
import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import Spacer, column, row
from bokeh.models import (ColumnDataSource, DataTable, DateFormatter,
                          HoverTool, Label, NumeralTickFormatter, Panel,
                          TableColumn, Tabs)
from bokeh.plotting import figure, output_file, save
from pyln.client import LightningRpc

### VARIABLES might needs to define!
lightning_rpc_path = Path.home() /'.lightning/bitcoin/lightning-rpc' # should be in .lightning folder, e.g. "/home/node/.lightning/bitcoin/lightning-rpc"
if  not lightning_rpc_path.exists():
    raise Exception(f'lightning-rpc is not found at {lightning_rpc_path}!, you need to set the current path' 
                     'in create_node_info.py')

# Analyse data from what date to what date
month,year = 4,2021 # will created data from this 
curr_m ,curr_y = datetime.now().month,datetime.now().year # till today.


## VARIABLES end


# get all needed data
rpc = LightningRpc(str(lightning_rpc_path))
nodes =  rpc.listnodes()      
peers = rpc.listpeers()
chans = rpc.listchannels()
owner_node = rpc.getinfo()['id']
forwards = rpc.listforwards()['forwards']
df_frwds = pd.DataFrame(rpc.listforwards()['forwards'])

df_prs = pd.DataFrame(peers['peers'])

chan_df = pd.DataFrame(chans['channels'])
id_to_alias = {}
for node in nodes['nodes']:
    if 'alias' in node:
        id_to_alias[node['nodeid']] = node['alias']
    else:
        id_to_alias[node['nodeid']] = 'un-def'

#Func 
def bitcoin_num(x):
    a = f'{x%100000000:08d}'
    #print(a)
    return f'{x//100000000}.{a[:2]},{a[2:5]},{a[5:9]}'


## create efficent dicts
chan_to_node = {}
chan_to_alias = {}
for chan in set(df_frwds['in_channel']):
    nodes = list(chan_df.query("short_channel_id==@chan")['source'])
    for node in nodes:
        if node != owner_node:
            chan_to_node[chan] = node
            chan_to_alias[chan] = id_to_alias[node]


## caclulate in/out liquidiy #
##############################

df = df_prs[df_prs['channels'].apply(lambda x:len(x))>0].copy() # get only peers with channels
df_chans = df.pop('channels')
df = pd.concat([df,pd.DataFrame([i[0] for i in df_chans.values])],axis=1)
df_t = df.query('state=="CHANNELD_NORMAL"').query("our_reserve_msat<to_us_msat")
to_us = (df_t['to_us_msat'] - df['our_reserve_msat']).sum()

df_t = df.query('state=="CHANNELD_NORMAL"').query("their_reserve_msat<total_msat-to_us_msat")
to_them = (df_t['total_msat']-df_t['to_us_msat'] - df['their_reserve_msat']).sum()


to_them, to_us

out_liqudity = bitcoin_num(int(round(float(to_them.to_satoshi()))))
in_liqudity = bitcoin_num(int(round(float(to_us.to_satoshi()))))



### create liquidy fig

node_name  = 'Test'
p = figure(plot_width=600, plot_height=100,toolbar_location=None)

p.add_layout(Label(x=50, y=85, text='Available',text_align='center'))


p.add_layout(Label(x=25, y=70, text='Out',text_align='center'))
p.add_layout(Label(x=75, y=70, text='In',text_align='center'))

p.add_layout(Label(x=25, y=50, text=out_liqudity,text_align='center'))
p.add_layout(Label(x=75, y=50, text=in_liqudity,text_align='center'))
p.line([50,50], [0,80], line_width=1, line_dash='dashed',line_color='black')
p.x_range.start = 0
p.x_range.end = 100
p.y_range.end = 100
p.y_range.start = 30
p.axis.visible = False
p.xgrid.grid_line_color = None
p.ygrid.grid_line_color = None

fig_liq = p


### Create forwards figs ###
############################

## reorgnize forwards dataframe
df = df_frwds
df['counts'] =1
df['date'] = pd.to_datetime(df['received_time'],unit='s')
df['day'] = df['date'].dt.day  
df['month'] = df['date'].dt.month
df['year'] = df['date'].dt.year

# get onlu settled forwards
df.query("status=='settled'").groupby(['year','month','day']).sum() 
df['in_channel_alias'] =  df['in_channel'].apply(lambda x: chan_to_alias[x] if x in chan_to_alias else 'un')
df['out_channel_alias'] =  df['out_channel'].apply(lambda x: chan_to_alias[x] if x in chan_to_alias else 'un')

#group by day
summary_df = df.query("status=='settled'").groupby(['year','month','day']).sum()
tabs_fee = []
tabs_forwards = []
tabs_amount = []

tabs_fee_per_m = []
tabs_forwards_per_m = []
tabs_amount_per_m = []

#run on each month
for dt in  pd.period_range(start=f'{year}-{month}',end=f'{curr_y}-{curr_m}', freq='M'):
    if (dt.year,dt.month) not in summary_df.index:
        continue # if there is no data for a specific month

    df_temp = summary_df.loc[dt.year,dt.month].copy()
    df_temp['in_sat'] = round(df_temp['in_msatoshi']/1000)
    df_temp['in_sats'] = df_temp['in_sat'].apply(lambda x:bitcoin_num(int(x)))
    df_temp['fee'] = df_temp['fee']/1000
    
    # creating 3 different plots, each with a differet y-axes 
    for top in ['counts','fee','in_sat']:
        p = figure(width=900, height=300)
        p.vbar(x='day', top=top, width=0.9, source=df_temp)
        p.x_range.start = 0.3
        p.x_range.end = 31.7
        p.xaxis.axis_label = f'{dt.month} - {dt.year}'
        if top == 'counts':
            p.add_tools(HoverTool(tooltips=[("total", "@in_sats"), ("fees", "@fee")]))
            p.yaxis.axis_label = '# Forwards'
            tabs = tabs_forwards
        elif top == 'fee':
            p.add_tools(HoverTool(tooltips=[("# Forwards", "@counts"), ("total", "@in_sats")]))
            p.yaxis.axis_label = 'Total fees (sats)'
            tabs = tabs_fee
        elif top =='in_sat':
            p.add_tools(HoverTool(tooltips=[("# Forwards", "@counts"), ("fees", "@fee")]))
            p.yaxis.axis_label = 'Total amount forwarded (sats)'
            p.yaxis.formatter=NumeralTickFormatter(format=",")
            tabs = tabs_amount

        tabs.append(Panel(child=p, title=f"{dt.year}-{dt.month}"))
        
forwards_count = Panel(child=Tabs(tabs=tabs_forwards), title=f"Forwards counts")
forwards_fees  = Panel(child=Tabs(tabs=tabs_fee), title=f"Forwards fees")
forwards_amount = Panel(child=Tabs(tabs=tabs_amount), title=f"Forwards amount")

all_both = Tabs(tabs=[forwards_count,forwards_fees,forwards_amount])
fig_forwards = all_both



## Create summary per month ##
df = df_frwds
summary_df_m = df.query("status=='settled'").groupby(['year','month']).sum()
summary_df_m['in_sat'] = round(summary_df_m['in_msatoshi']/1000)
summary_df_m['in_sats'] = summary_df_m['in_sat'].apply(lambda x:bitcoin_num(int(x)))
summary_df_m['fee'] = summary_df_m['fee']/1000

tabs = []
for y,label  in [[summary_df_m.counts,"Forwards counts"], [summary_df_m.fee,"Forwards fees"],[summary_df_m.in_sat,"Forwards amount"]]:
    x = [datetime(i[0],i[1],1) for i in summary_df_m.index]
    p = figure(width=900, height=300,x_axis_type='datetime')
    p.line(x, y, line_width=2)
    if label == "Forwards counts":
        p.yaxis.axis_label = '# Forwards'
    elif label == "Forwards fees":
        p.yaxis.axis_label = 'Total fees (msats)'
    elif label =="Forwards amount":
        p.yaxis.axis_label = 'Total amount forwarded (sats)'
        p.yaxis.formatter=NumeralTickFormatter(format=",")
    tabs.append(Panel(child=p, title=label))
summary_m = Tabs(tabs=tabs)


##  Create table with forawrds per month per channel ##
#######################################################
df = df_frwds
summary_df = df.query("status=='settled'").groupby(['year','month','day']).sum()
tabs =[]
for dt in  pd.period_range(start=f'{year}-{month}',end=f'{curr_y}-{curr_m}', freq='M'):
    if (dt.year,dt.month) not in summary_df.index:
        continue
    source_in = df.query("status=='settled'").groupby(['year','month','in_channel']).sum().loc[dt.year,dt.month]
    source_out = df.query("status=='settled'").groupby(['year','month','out_channel']).sum().loc[dt.year,dt.month]

    # I get free from in or out????????
    source_in['in_sat'] = round(source_in['in_msatoshi']/1000) + round(source_in['in_msatoshi']/1000)
    source_in['in_sats'] = source_in['in_sat'].apply(lambda x:bitcoin_num(int(x)))

    source_out['out_sat'] = round(source_out['out_msatoshi']/1000) + round(source_out['out_msatoshi']/1000)
    source_out['out_sats'] = source_out['out_sat'].apply(lambda x:bitcoin_num(int(x)))

    source_out['fee'] = source_out['fee']

    source_in['counts_in'] = source_in['counts']
    source_out['counts_out'] = source_out['counts']
    source_both = pd.concat([source_in,source_out],axis=1)
    source_both['total_counts'] = source_both.fillna(0)['counts_out'] + source_both.fillna(0)['counts_in']

    source_both = source_both.reset_index().sort_values('total_counts',ascending=False)

    source_both['chan_name'] = source_both['index'].apply(lambda x: chan_to_alias[x] if x in chan_to_alias else 'Un')
    source = ColumnDataSource(source_both.fillna(0))
    columns = [
        TableColumn(field="chan_name", title="channel"),
            TableColumn(field="total_counts", title="# Forwards"),
            TableColumn(field="in_sats", title="In_amount"),
            TableColumn(field="out_sats", title="Out_amount"),
            TableColumn(field="fee", title="Fees (mSats)", formatter=bokeh.models.NumberFormatter(format=","))
            ]
    
    data_table = DataTable(source=source, columns=columns, width=600, height=880)
    tabs.append(Panel(child=data_table, title=f"{dt.year}-{dt.month}"))

chart_tabs = Tabs(tabs=tabs)





## Now merge all plots together and save as index.html
curdoc().clear()
node_name = id_to_alias[owner_node]
output_file("index.html", title=f"Node - {node_name} stats")

spr0 = Spacer(width=400, height=50, sizing_mode='scale_width')
spr1 = Spacer(width=400, height=50, sizing_mode='scale_width')
spr2 = Spacer(width=5, height=50)
spr3 = Spacer(width=400, height=5)

cl_r = column([spr0,fig_liq,spr1, chart_tabs])
cl_l = column([fig_forwards, spr3,summary_m])
rl = row([cl_r,spr2,cl_l])

save(rl)


