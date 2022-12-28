import datetime
import time
import re
from decimal import *
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy.orm import declarative_base
from sqlalchemy import Date
Base = declarative_base()
# Used to get the sql alchemy metadata of the classes in this file
def getBase():
    return Base

class alchemy_class_sql_job(Base):
    __tablename__ = "shredded_job"

    job_id = Column(String(100), primary_key=True)

    job_name = Column(String(255))#
    queue_name = Column(String(255))#
    user_name = Column(String(255))#
    group_name = Column(String(255))#
    pi_name = Column(String(255))
    start_time = Column(Integer)     # Timestamp
    end_time = Column(Integer)       # Timestamp
    submission_time = Column(Integer)     # Timestamp
    eligible_time = Column(Integer)       # Timestamp
    wall_time = Column(Integer) # Secs
    wait_time = Column(Integer) # Secs
    node_count = Column(Integer)
    cpu_count = Column(Integer)
    gpu_count = Column(Integer)
    cpu_req = Column(Integer)
    mem_req = Column(String(50))
    node_list = Column(String(64000))


# Used to store info about jobs that is shared across nodes
class sql_job:

    def __init__(self, id, job_obj):
        self.job_id = id
        self.job_name = job_obj.get_data("jobname")
        self.queue_name = job_obj.get_data("queue")
        self.user_name = job_obj.get_data("user")
        self.group_name = job_obj.get_data("group")
        self.pi_name = job_obj.get_data("group")
        self.start_time = job_obj.get_data("start")
        self.end_time = job_obj.get_data("end")
        self.submission_time = job_obj.get_data("ctime")
        self.eligible_time = job_obj.get_data("etime")
        self.wall_time = int(job_obj.get_data("end")) - int(job_obj.get_data("start"))
        self.wait_time = int(job_obj.get_data("start")) - int(job_obj.get_data("ctime"))
        self.node_count = job_obj.get_resource_list("nodect")
        self.cpu_count = job_obj.get_resource_list("ncpus")
        self.gpu_count = job_obj.get_resource_list("ngpus")
        self.cpu_req = job_obj.get_resource_list("ncpus")
        self.mem_req = job_obj.get_resource_list("mem")
        self.node_list = job_obj.get_data("exec_vnode")
        


    def export_to_alchemy(self):
        return alchemy_class_sql_job(
                job_id = self.job_id,
                job_name = self.job_name,
                queue_name = self.queue_name,
                user_name = self.user_name,
                group_name = self.group_name,
                pi_name = self.pi_name,
                start_time = self.start_time,
                end_time = self.end_time,
                submission_time = self.submission_time,
                eligible_time = self.eligible_time,
                wall_time = self.wall_time,
                wait_time = self.wait_time,
                node_count = self.node_count,
                cpu_count = self.cpu_count,
                gpu_count = self.gpu_count,
                cpu_req = self.cpu_req,
                mem_req = self.mem_req,
                node_list = self.node_list
        )


# A class for data parsing of the accounting logs
class Job():



    # The ID must be a valid pbs jobid and the data_dict should be a dictionary representation of the accounting log E record
    # associated with the job, with the Resource_List and resources_used stored as sub dictionaries  
    def __init__(self, id, data_dict):
        # Theses keys must be in the data_dict
        required_keys = [
                            'user', 'exec_host', 
                            'exec_vnode', 'group', 
                            'end', 'start', 'ctime', 
                            'qtime', 'etime', 
                            'Exit_status','exec_host', 
                            'queue', 'jobname', 
                            'session', 'run_count', 
                            'resources_used', 'Resource_List'
                           ]
    
        self.jobid = id
        # Checks for keys
        for key in required_keys:
            if key not in data_dict:
                raise KeyError("Missing " + key + " in data")
    
        self.data_dict = data_dict

    def get_data(self, tag):
        return self.data_dict[tag]

    def get_id(self):
        return self.jobid
    
    def get_resource_used(self, tag):
        return self.data_dict["resources_used"][tag]

    def get_resource_list(self, tag):
        return self.data_dict["Resource_List"][tag]

class jobfact_by_day(Base):
    __tablename__ = "jobfact_by_day"
    
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    user = Column(String(255))
    group = Column(String(255))
    queue = Column(String(255))
    node_count = Column(Integer)
    processor_count = Column(Integer)
    gpu_count = Column(Integer)
    submitted_job_count = Column(Integer)
    ended_job_count = Column(Integer)
    running_job_count = Column(Integer)
    started_job_count = Column(Integer)
    wallduration = Column(Integer)
    waitduration = Column(Integer)
    cpu_time = Column(Integer)
    node_time = Column(Integer)
    gpu_time = Column(Integer)
    mem_time = Column(Integer)


class aggobj:

    def __init__(self, keyStringList):
        date, username, queue, grp ,node_count, processor_count, gpu_count = keyStringList
        self.date = date
        self.user = username
        self.queue = queue
        self.grp = grp
        self.nodect = node_count
        self.ncpus = processor_count
        self.ngpus = gpu_count
        self.wall_hours = 0
        self.wait_hours = 0
        self.cpu_hours = 0
        self.gpu_hours = 0
        self.node_hours = 0
        self.mem_hours = 0
        self.jobct = 0
        self.ended_jobs = 0
        self.started_jobs = 0
        self.submitted_jobs = 0
    
    def export(self):
        return alchemy_class_powerbi(date=self.date,
                                user=self.user,
                                group=self.grp,
                                queue=self.queue,
                                node_count=self.nodect,
                                processor_count=self.ncpus,
                                gpu_count=self.ngpus,
                                submitted_job_count = self.jobct,
                                ended_job_count = self.ended_jobs,
                                wall_hours = self.wall_hours,
                                wait_hours = self.wait_hours,
                                cpu_hours = self.cpu_hours,
                                gpu_hours = self.gpu_hours,
                                node_hours = self.node_hours,mem_hours = self.mem_hours)

    def export_fact(self):
        return jobfact_by_day(
                       date=self.date,
                        group=self.grp,
                        user=self.user,
                        queue=self.queue,
                        node_count=self.nodect,
                        processor_count=self.ncpus,
                        gpu_count=self.ngpus,
                        submitted_job_count = self.submitted_jobs,
                        ended_job_count = self.ended_jobs,
                        running_job_count = self.jobct,
                        started_job_count = self.started_jobs,
                        wallduration = self.wall_hours,
                        waitduration = self.wait_hours,
                        cpu_time = self.cpu_hours,
                        gpu_time = self.gpu_hours,
                        node_time = self.node_hours,
                        mem_time = self.mem_hours
        )

class alchemy_class_powerbi(Base):
    __tablename__ = "powerbi"

    date = Column(Date, primary_key=True)   #KeyX
    grp = Column(String(255))               #KeyX
    username = Column(String(100))          #KeyX
    node_count = Column(Integer)            #KeyX
    processor_count = Column(Integer)       #KeyX
    gpu_count = Column(Integer)             #KeyX
    submitted_job_count = Column(Integer)   #Sum
    wall_hours = Column(Float)              #Sum
    wait_hours = Column(Float)              #Sum
    cpu_hours = Column(Float)               #Sum
    gpu_hours = Column(Float)               #Sum
    node_hours = Column(Float)              #Sum
    mem_hours = Column(Float)