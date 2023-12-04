"""
For 2 DIMMs results
"""
import pandas as pd
import numpy as np

    
leanstore_csv = pd.read_csv("LEANSTORE_1th_24GB_100WH_MASTER_10MIN_dstat.csv", skiprows=5, index_col=False)
artlsm_csv = pd.read_csv("artlsm_artree_rocksdb_1th_24GB_100WH_cf_compaction_dstat.csv", skiprows=5, index_col=False)
print(leanstore_csv)

import matplotlib.pyplot as plt
import sys
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
from matplotlib.ticker import FuncFormatter
import matplotlib
# plt.rcParams["font.family"] = "arial"
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

gloyend = None
# Main
dashes=[(2,2), (4,1), (2,0), (2,0), (3, 3), (2, 0), (2,2), (4,1), (2,0), (2,0), (3, 3), (2, 0)]
markers = ['x', '|', '.', 'D', 'd', '', 'x', '|', '.', 'D', 'd', '']
colors = ['#d7191c', '#2c7bb6', '#ffffbf', '#1a9641']
# colors = ['#2077B4', '#D62728', '#0A640C', '#343434']

cpulabel = ['usr', 'sys', 'idl', 'wai']
memlabel = ['used', "free", "buff", "cach"]

# d0 d1 represents dynamic CCEH data
# f0 f1 represents fixed CCEH data

fig, ax = plt.subplots(6, figsize=(8, 8.8), constrained_layout=True, sharex=True, sharey=False)
fig.suptitle('dstat of Leanstore')

iwidth=5
ax[0].bar(leanstore_csv.index, leanstore_csv['usr'], label = cpulabel[0], color = colors[0],width = iwidth)
ax[0].bar(leanstore_csv.index, leanstore_csv['sys'], bottom = leanstore_csv['usr'], label = cpulabel[1], color = colors[1],width = iwidth)
ax[0].bar(leanstore_csv.index, leanstore_csv['idl'], bottom = leanstore_csv['usr']+leanstore_csv['sys'], label = cpulabel[2], color = colors[2],width = iwidth)
ax[0].bar(leanstore_csv.index, leanstore_csv['wai'], bottom = leanstore_csv['usr']+leanstore_csv['sys']+leanstore_csv['idl'], label = cpulabel[3], color = colors[3],width = iwidth)

ax[0].legend(bbox_to_anchor=(1.1, 1.05))
ax[0].grid(which='major', linestyle='--', zorder=0)
ax[0].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[0].xaxis.grid(False, which='both')

ax[0].set_xlabel('Time Epoch (sec)', fontsize=12)
ax[0].set_ylabel('CPU Usage (%)', fontsize=12)
ax[0].set_title('Leanstore (CPU Usage) Uniform Random', fontsize=14)
ax[0].set_ylim(0,100)

ax[1].bar(artlsm_csv.index, artlsm_csv['usr'], label = cpulabel[0], color = colors[0],width = iwidth)
ax[1].bar(artlsm_csv.index, artlsm_csv['sys'], bottom = artlsm_csv['usr'], label = cpulabel[1], color = colors[1],width = iwidth)
ax[1].bar(artlsm_csv.index, artlsm_csv['idl'], bottom = artlsm_csv['usr']+artlsm_csv['sys'], label = cpulabel[2], color = colors[2],width = iwidth)
ax[1].bar(artlsm_csv.index, artlsm_csv['wai'], bottom = artlsm_csv['usr']+artlsm_csv['sys']+artlsm_csv['idl'], label = cpulabel[3], color = colors[3],width = iwidth)

ax[1].legend(bbox_to_anchor=(1.1, 1.05))
ax[1].grid(which='major', linestyle='--', zorder=0)
ax[1].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[1].xaxis.grid(False, which='both')

ax[1].set_xlabel('Time Epoch (sec)', fontsize=12)
ax[1].set_ylabel('CPU Usage (%)', fontsize=12)
ax[1].set_title('ARTLSM (CPU Usage) Uniform Random', fontsize=14)
ax[1].set_ylim(0,100)

ax[2].bar(leanstore_csv.index, leanstore_csv['used']/1024/1024/1024, label = memlabel[0], color = colors[0],width = iwidth)
ax[2].bar(leanstore_csv.index, leanstore_csv['free']/1024/1024/1024, bottom = leanstore_csv['used']/1024/1024/1024, label = memlabel[1], color = colors[1],width = iwidth)
ax[2].bar(leanstore_csv.index, leanstore_csv['buff']/1024/1024/1024, bottom = leanstore_csv['used']/1024/1024/1024+leanstore_csv['free']/1024/1024/1024, label = memlabel[2], color = colors[2],width = iwidth)
ax[2].bar(leanstore_csv.index, leanstore_csv['cach']/1024/1024/1024, bottom = leanstore_csv['used']/1024/1024/1024+leanstore_csv['free']/1024/1024/1024+leanstore_csv['buff']/1024/1024/1024, label = memlabel[3], color = colors[3],width = iwidth)

ax[2].legend(bbox_to_anchor=(0.95, 1.05))
ax[2].grid(which='major', linestyle='--', zorder=0)
ax[2].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[2].xaxis.grid(False, which='both')

ax[2].set_xlabel('Time Epoch (sec)', fontsize=12)
ax[2].set_ylabel('Memory Usage (GB)', fontsize=12)
ax[2].set_title('Leanstore (Memory Usage) Uniform Random', fontsize=14)
ax[2].set_ylim(0,30)

ax[3].bar(artlsm_csv.index, artlsm_csv['used']/1024/1024/1024, label = memlabel[0], color = colors[0],width = iwidth)
ax[3].bar(artlsm_csv.index, artlsm_csv['free']/1024/1024/1024, bottom = artlsm_csv['used']/1024/1024/1024, label = memlabel[1], color = colors[1],width = iwidth)
ax[3].bar(artlsm_csv.index, artlsm_csv['buff']/1024/1024/1024, bottom = artlsm_csv['used']/1024/1024/1024+artlsm_csv['free']/1024/1024/1024, label = memlabel[2], color = colors[2],width = iwidth)
ax[3].bar(artlsm_csv.index, artlsm_csv['cach']/1024/1024/1024, bottom = artlsm_csv['used']/1024/1024/1024+artlsm_csv['free']/1024/1024/1024+artlsm_csv['buff']/1024/1024/1024, label = memlabel[3], color = colors[3],width = iwidth)

ax[3].legend(bbox_to_anchor=(0.95, 1.05))
ax[3].grid(which='major', linestyle='--', zorder=0)
ax[3].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[3].xaxis.grid(False, which='both')

ax[3].set_xlabel('Time Epoch (sec)', fontsize=12)
ax[3].set_ylabel('Memory Usage (GB)', fontsize=12)
ax[3].set_title('ARTLSM (Memory Usage) Random', fontsize=14)
ax[3].set_ylim(0,30)

ax[4].plot(leanstore_csv.index, leanstore_csv['dsk/sda:writ']/1024/1024, label = "leanstore io/writ random", color = colors[0])
ax[4].plot(artlsm_csv.index, artlsm_csv['dsk/sda:writ']/1024/1024, label = "artlsm io/writ Random", color = colors[1])

ax[4].legend(bbox_to_anchor=(0.95, 1.05))
ax[4].grid(which='major', linestyle='--', zorder=0)
ax[4].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[4].xaxis.grid(False, which='both')

ax[4].set_xlabel('Time Epoch (sec)', fontsize=12)
ax[4].set_ylabel('io/write (MB)', fontsize=12)
ax[4].set_title('Write I/O', fontsize=14)


ax[5].plot(leanstore_csv.index, leanstore_csv['dsk/sda:read']/1024/1024, label = "leanstore io/read random", color = colors[0])
ax[5].plot(artlsm_csv.index, artlsm_csv['dsk/sda:read']/1024/1024, label = "artlsm io/read Random", color = colors[1])

ax[5].legend(bbox_to_anchor=(0.95, 1.05))
ax[5].grid(which='major', linestyle='--', zorder=0)
ax[5].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[5].xaxis.grid(False, which='both')

ax[5].set_xlabel('Time Epoch (sec)', fontsize=12)
ax[5].set_ylabel('io/read (MB)', fontsize=12)
ax[5].set_title('Read I/O', fontsize=14)




fig.savefig("./artlsm_artree_rocksdb_1th_24GB_100WH_cf_compaction_dstat.png", bbox_inches='tight', pad_inches=0)