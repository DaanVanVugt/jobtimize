#!/usr/bin/env python
"""
Program to draw an estimate of the job server scheduling

Required software: Matplotlib, palettable
"""
from datetime import datetime, timedelta
import subprocess
import re
import json
import sys
import getpass
from itertools import groupby, count
from collections import OrderedDict
import palettable
import matplotlib
from matplotlib import patches, pyplot as plt

VAR_RE = re.compile(r"    ([^ =]*) = ")
CMAP = palettable.colorbrewer.qualitative.Paired_12
CMAP2 = palettable.colorbrewer.qualitative.Pastel1_6
CMAP3 = palettable.colorbrewer.qualitative.Dark2_8
NODE_TYPE = "xfua" # or "xfuaknl"

class Job:
    """Represents a single PBS job with minimal parsing from qstat -f command"""
    job_id = 0
    job_name = ""
    job_state = ""
    job_owner = "" # stripped from hostname
    queue = ""
    nodect = 0
    walltime = timedelta()
    eligible_time = datetime.now()
    exec_vnode = []
    start_time = datetime.now()

def get_xfua_jobs():
    """Get all job ids from qstat and grep for xfua"""
    return [job_from_qstat(job) for job in subprocess.run(["qstat", "-f"], stdout=subprocess.PIPE).stdout.decode('utf-8').split("\n\n")]

def interval(s):
    "Converts a string to a timedelta"
    d = re.match(r'((?P<days>\d+) days, )?(?P<hours>\d+):'
                 r'(?P<minutes>\d+):(?P<seconds>\d+)', str(s)).groupdict(0)
    return timedelta(**dict(((key, int(value)) for key, value in d.items())))

def job_from_qstat(job_info):
    """Parse output from qstat -f {id} and return a Job"""
    j = Job()
    # job id is in first line
    j.job_id = job_info.split("\n")[0].split(":")
    if len(j.job_id) == 2: j.job_id = j.job_id[1].strip()
    exec_vnode = False
    has_stime = False
    # Hacky way to read this kind of output.
    in_vnode = False
    est_vnode = ""
    for line in job_info.split("\n")[1:-1]:
        # if indented by 4 spaces its a variable definition
        varmatch = re.match(VAR_RE, line)
        if varmatch:
            in_vnode = False
            varname = varmatch.group(1)
            rest = line.replace(varmatch.group(0), '', 1)
            if varname == "Job_Name":
                j.job_name = rest
            elif varname == "Job_Owner":
                j.job_owner = rest.split("@")[0]
            elif varname == "job_state":
                j.job_state = rest
            elif varname == "queue":
                j.queue = rest
                if rest.find(NODE_TYPE) == -1:
                    return None
            elif varname == "Resource_List.nodect":
                j.nodect = int(rest)
            elif varname == "Resource_List.walltime":
                j.walltime = interval(rest)
            elif varname == "eligible_time":
                try:
                    j.eligible_time = datetime.strptime(rest, "%c")
                except ValueError:
                    j.eligible_time = datetime.now() # 0 if already started
            elif varname == "stime":
                has_stime = True
                try:
                    j.start_time = datetime.strptime(rest, "%c")
                except ValueError:
                    pass
            elif varname == "estimated.start_time" and not has_stime:
                try:
                    j.start_time = datetime.strptime(rest, "%c")
                except ValueError:
                    pass
            elif varname == "exec_vnode":
                in_vnode = True
                exec_vnode = True
                est_vnode = rest
            elif varname == "estimated.exec_vnode" and not exec_vnode: # so that we do not overwrite exec_vnode with est_vnode
                in_vnode = True
                est_vnode = rest
        elif in_vnode:
            est_vnode = est_vnode + line.strip()
    # finally clean up estimated_vnode
    j.exec_vnode = [s.split(':')[0].strip('(') for s in est_vnode.split('+')]
    if len(j.exec_vnode) == 1 and j.exec_vnode[0] == '':
        j.exec_vnode = []
    return j

def get_compute_node_list():
    """Get a list of all nodes that are up"""
    nodes = json.loads(subprocess.run(["pbsnodes", "-aF", "json"],
                                      stdout=subprocess.PIPE).stdout.decode('utf-8'))["nodes"]
    return OrderedDict((k,v) for k, v in nodes.items() if
            v["resources_available"]["Qlist"].find(NODE_TYPE) >= 0)

def plot_job_schedule(jobs):
    """Color all of the compute nodes by their job over time. X axis is in hours"""
    nodes = get_compute_node_list()
    fig, ax = plt.subplots(1, 1, figsize=(19.2,10.8)) # for HD?
    ax.set_ylim([0,len(nodes)])
    ax.set_xlim([0,24])

    # Show mcdram and NUMA type if knl
    if (len(jobs) > 0 and jobs[0].queue.index('knl') >= 0):
        c_norm2 = matplotlib.colors.Normalize(0, 1)
        for i, (k,v) in enumerate(nodes.items()):
            i_c = 1*(v["resources_available"]["mcdram"] == "flat")
            color_i = CMAP2.mpl_colormap(c_norm2(i_c))
            if (i_c == 0):
                hatch='/'
            else:
                hatch='\\'
            ax.add_patch(patches.Rectangle(
                (0,i), # (x,y)
                24, 1, # w, h
                facecolor=color_i, hatch=hatch))

        # manual labels
        ax.annotate("cache", (12, 1), # (x,y)
                    color='black', fontsize='10', ha='center', va='bottom')
        ax.annotate("flat", (12, len(nodes)), # (x,y)
                    color='black', fontsize='10', ha='center', va='top')

    # Show offline nodes
    for i, (k,v) in enumerate(nodes.items()):
        if ('offline' in v["state"]):
            ax.add_patch(patches.Rectangle(
                (0,i), # (x,y)
                24, 1, # w, h
                facecolor='grey'))


    username = getpass.getuser()
    n_user = sum([len(job.exec_vnode) > 0 and job.job_owner == username for job in jobs])
    n_other = sum([len(job.exec_vnode) > 0 and job.job_owner != username for job in jobs])
    i_user = 0
    i_other = 0
    c_norm = matplotlib.colors.Normalize(1, n_other)
    c_norm3 = matplotlib.colors.Normalize(1, n_user)
    for i, job in enumerate(jobs):
        if len(job.exec_vnode) > 0 and job.job_state != 'H':
            i_nodes = [list(nodes.keys()).index(nodename) for nodename in job.exec_vnode]
            # Convert these into the minimum number of sequences
            tstart = job.start_time
            tend = job.start_time + job.walltime
            tstart = (tstart - datetime.now()).total_seconds()/3600
            tend   = (tend - datetime.now()).total_seconds()/3600

            # Group node numbers to draw fewer rectangles
            if job.job_owner == username:
                i_user = i_user + 1
                color_i = CMAP3.mpl_colormap(c_norm3(i_user)) # rgba value
            else:
                i_other = i_other + 1
                color_i = CMAP.mpl_colormap(c_norm(i_other)) # rgba value
            ranges = groupby(sorted(i_nodes), lambda n, c=count(): n-next(c))
            for k,v in ranges:
                r = list(v)
                ax.add_patch(patches.Rectangle(
                    (tstart,r[0]), # (x,y)
                    tend-tstart, r[-1]-r[0]+1, # w, h
                    facecolor=color_i))
                if r[-1]-r[0] > 0: # otherwise no space
                    ax.annotate("%s: %s (%s)"%(job.job_id.split('.')[0], job.job_name, job.job_owner), (max((tstart+tend)/2,0), (r[0]+r[-1])/2), # (x,y)
                                color='black', fontsize='6', ha='center', va='center')
    plt.tight_layout()
    plt.savefig('jobs.png', dpi=100)



if __name__ == "__main__":
    if (len(sys.argv) > 1):
        NODE_TYPE = sys.argv[1]
    plot_job_schedule([job for job in get_xfua_jobs() if job is not None])
