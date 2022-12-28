import datetime
import time
import re
from decimal import *
from classes import aggobj, alchemy_class_sql_job, getBase


from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import select

def get_jobs():
    connection_string="postgresql://postgres:password@localhost/shredtarget"

    engine = create_engine(connection_string)
    
    with Session(engine) as sess:
        s = select(alchemy_class_sql_job)
        result = sess.execute(s)
            
        jobs = []
        for r in result:
            obj = r[0]
            jobs.append(obj)
        return jobs



def jobs_to_aggs(jobs):
    stimes = []
    etimes = []
    output = {}

    for j in jobs:
        stimes.append(j.start_time)
        etimes.append(j.end_time)
    min_stime = min(stimes)
    max_etime = max(etimes)
    start_date = datetime.date.fromtimestamp(min_stime)
    end_date = datetime.date.fromtimestamp(max_etime)
    print( start_date, end_date)
    dif = end_date - start_date

    dates = [end_date - (x * datetime.timedelta(1,0,0,0,0,0,0)) for x in range(dif.days+1)]    
    #For every day in between the jobs there is a dic for the aggs
    for d in dates:
        output[d] = {}
    #
    

    if True:
        for j in jobs:
            #Values to use as keys
            start_date = datetime.date.fromtimestamp(j.start_time)
            end_date = datetime.date.fromtimestamp(j.end_time)
            dif = end_date - start_date

            daysofthisjob = [end_date - (x * datetime.timedelta(1,0,0,0,0,0,0)) for x in range(dif.days+1)]

            keyValues = []
            keyValues.append(j.user_name)
            keyValues.append(j.queue_name)
            keyValues.append(j.group_name)
            keyValues.append(str(j.node_count))
            keyValues.append(str(j.cpu_count))
            keyValues.append(str(j.gpu_count))
            keyString = ":".join(keyValues)
            for date in daysofthisjob:
                if keyString not in output[date]:
                    output[date][keyString] = aggobj([date]+keyString.split(":"))
            
                basewalltime = Decimal(j.end_time) - Decimal(j.start_time)
                
                window_start = time.mktime(datetime.datetime.combine(date, datetime.time.min).timetuple())
                window_stop = time.mktime(datetime.datetime.combine(date, datetime.time.max).timetuple())

                job_start = j.start_time
                job_stop = j.end_time

                if window_start <= job_start and job_stop <= window_stop:
                    walltime = basewalltime
                elif (window_start >= job_start and job_stop <= window_stop ):
                    walltime = Decimal(basewalltime) * (Decimal(job_stop-window_start)/Decimal((job_stop - job_start)))
                elif ( window_start <= job_start and job_stop >= window_stop ):
                    walltime = Decimal(basewalltime) * (Decimal(window_stop-job_start + 1) / Decimal(job_stop - job_start))
                elif window_start >= job_start and job_stop >= window_stop:
                    walltime = Decimal(basewalltime) * Decimal(window_stop - window_start) / Decimal(job_stop - job_start)

                walltime = Decimal(0) if walltime < 0 else walltime
                if date != datetime.date.fromtimestamp(job_start):
                    wait_hours = 0
                else:
                    wait_hours = j.wait_time


                output[date][keyString].wall_hours += walltime
                output[date][keyString].wait_hours += wait_hours
                output[date][keyString].cpu_hours += walltime  * Decimal(j.cpu_count)
                output[date][keyString].gpu_hours += walltime  * Decimal(j.gpu_count)
                output[date][keyString].node_hours += walltime * Decimal(j.node_count)

                output[date][keyString].ended_jobs += 1 if end_date == date else 0
                output[date][keyString].started_jobs += 1 if start_date == date else 0
                output[date][keyString].submitted_jobs += 1 if datetime.date.fromtimestamp(j.submission_time) == date else 0

                #Support for mem reqs with b, kb, mb, gb, tb, and pb suffixes
                #(Output is in gb secs)
                base = 1024
                multi = {
                    ' ': base ** 0,
                    'k': base ** 1,
                    'm' : base ** 2,
                    'g': base ** 3,
                    't': base ** 4,
                    'p': base ** 5
                }
                mem_string = j.mem_req[:-1]
                mem, suffix = mem_string[:-1], mem_string[-1]
                mem_used = Decimal(mem) * Decimal(multi[suffix])
                gb_secs = ( mem_used / Decimal(1024) ** Decimal(3) ) * walltime

                output[date][keyString].mem_hours += gb_secs

                output[date][keyString].jobct += 1

        ret = []

        for d in output.values():
            for agg in d.values():
                ret.append(agg.export_fact())
        return ret

        '''
        users = {}
        for date in output.keys():


            for agg in output[date].values():
                if agg.user not in users:
                    users[agg.user] = agg.cpu_hours
                else:
                    users[agg.user] += agg.cpu_hours

        for name in sorted(users.keys()):
            print(name, users[name])'''

if __name__ == "__main__":
    jobs = get_jobs()
    with open("../shred_jobs.csv") as f:
        lines = f.read().split("\n")
        new_jobs = []
        i = 0
        for l in lines[1:-1]:
            
            i += 1
            linesd,_,datekey,job_id_r,_,_,j_name,_,queue,_,user,_,group,_,_,_,_,starttime,endtime,submit_time,etime,walltime,waittime,_,_,nodect,ncpu,ngpu,cpureq,memreq,timelimit,nodes = l.split(",")
            obj = alchemy_class_sql_job(
                job_id = job_id_r,
                job_name = j_name,
                queue_name = queue,
                user_name = user,
                group_name = group,
                pi_name = group,
                start_time = int(starttime),
                end_time = int(endtime),
                submission_time = int(submit_time),
                eligible_time = int(etime),
                wall_time= int(walltime),
                wait_time = int(waittime),
                node_count = int(nodect),
                cpu_count = int(ncpu),
                gpu_count = int(ngpu),
                cpu_req = int(cpureq),
                mem_req = memreq+" b",
            )
            new_jobs.append(obj)


    aggs = jobs_to_aggs(new_jobs)


    connection_string = "postgresql://postgres:password@localhost/shredtarget"
    engine = create_engine(connection_string)
    metadata = getBase().metadata
    metadata.create_all(engine, checkfirst=True)
    with Session(engine) as sess:
        for agg in aggs:
            sess.merge(agg)
        sess.commit()


