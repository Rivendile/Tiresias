from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# import numpy
import math
import util
import models
import csv
import time
import sys
import random
import flags
import copy
import itertools
FLAGS = flags.FLAGS

class _Packing(object):
    class _MiniJob(object):
        '''
        mini job for class:packing
        '''
        def __init__(self, rjob):
            self.num_gpu = rjob['num_gpu']
            self.resource_time = rjob['resource_time']
            self.job_idx = rjob['job_idx']
        def calc_iter_time(self):
            return sum(self.resource_time)

    def __init__(self, rjob):
        self.packing_jobs = list()
        job_tmp = self._MiniJob(rjob)
        self.packing_jobs.append(job_tmp)
        self.num_gpu = job_tmp.num_gpu

    def add_job(self, rjob):
        self.packing_jobs.extend(rjob.packing_jobs)

    def calc_iteration_time(self, permutation):
        TT = 0.0
        for i in range(FLAGS.multi_resource):
            max_num = 0.0
            for idx, val in enumerate(permutation):
                # print(i, idx, FLAGS.multi_resource, (i-idx+FLAGS.multi_resource)%FLAGS.multi_resource)
                max_num = max(max_num, val.resource_time[(i-idx+FLAGS.multi_resource)%FLAGS.multi_resource])
            TT += max_num
        return TT

    def calc_used_ratio(self, packing):
        TT = float("inf")
        jobs = self.packing_jobs + packing.packing_jobs
        jobs_permutation = itertools.permutations(jobs)
        for permutation in jobs_permutation:
            TT = min(TT, self.calc_iteration_time(permutation))
        
        used_time = [sum(i.resource_time) for i in jobs]
        used_time_sum = sum(used_time)

        return used_time_sum/(TT*FLAGS.multi_resource)
    

class _Matching_Split(object):
    '''
    matching algorithm for multi-resource packing
    will split job if #GPU is not matched
    '''
    def __init__(self):
        self.run_jobs_list = list()

        # params for matching
        #int
        self.match = list()
        self.tt = 0
        self.si = list()
        self.ti = list()
        #double
        self.graph_weight = list()
        self.lx = list()
        self.ly = list()
        self.sla = list()

    def KM_GPU_num(self, node):
        node_gpu = dict()
        for rnode in node:
            num_gpu = rnode.num_gpu
            if num_gpu not in node_gpu:
                node_gpu[num_gpu] = list()
            
            node_gpu[num_gpu].append(rnode)
        return node_gpu

    # find path for KM
    def find(self, idx, left_size, right_size):
        self.si[idx] = self.tt
        for i in range(right_size):
            if self.ti[i] == self.tt:
                continue
            if math.isclose(self.lx[idx]+self.ly[i], self.graph_weight[idx][i], rel_tol=1e-6):
                self.ti[i] = self.tt
                if self.match[i]==-1:
                    find_flag = self.find(self.match[i], left_size, right_size)
                    if find_flag:
                        self.match[i] = idx
                        return True
                else:
                    self.sla[i] = min(self.sla[i], self.lx[idx]+self.ly[i]-self.graph_weight[idx][i])
        return False

    # update for KM
    def update(self, left_size, right_size):
        tmp_val = min(self.sla[i] for i in range(right_size) if self.ti[i]!=self.tt)
        for i in range(left_size):
            if self.si[i] == self.tt:
                self.lx[i] -= tmp_val
        for i in range(right_size):
            if self.ti[i] == self.tt:
                self.ly[i] += tmp_val
            else:
                self.sla[i] -= tmp_val

    # KM algorithm
    def KM_one_round(self, left_node_gpu, right_node_gpu):
        left_gpu_num = list(left_node_gpu.keys())
        right_gpu_num = list(right_node_gpu.keys())
        gpu_num_list = left_gpu_num + right_gpu_num
        gpu_num_list = list(set(gpu_num_list))
        gpu_num_list.sort(reverse=True)

        for gpu_num in gpu_num_list:
            # deal with special case that gpu_num-job is not in one side
            if gpu_num == 1 and ((gpu_num not in left_gpu_num) or (gpu_num not in right_gpu_num)):
                continue
            if gpu_num not in left_gpu_num:
                for rjob in right_node_gpu[gpu_num]:
                    split_job0 = copy.deepcopy(rjob)
                    split_job0.num_gpu /= 2
                    split_job1 = copy.deepcopy(split_job0)
                    right_node_gpu[gpu_num/2].append(split_job0)
                    right_node_gpu[gpu_num/2].append(split_job1)
                    right_node_gpu[gpu_num].remove(rjob)
                continue
            if gpu_num not in right_gpu_num:
                for rjob in left_node_gpu[gpu_num]:
                    split_job0 = copy.deepcopy(rjob)
                    split_job0.num_gpu /= 2
                    split_job1 = copy.deepcopy(split_job0)
                    left_node_gpu[gpu_num/2].append(split_job0)
                    left_node_gpu[gpu_num/2].append(split_job1)
                    left_node_gpu[gpu_num].remove(rjob)
                continue

            # build the graph
            left_size = len(left_node_gpu[gpu_num])
            right_size = len(right_node_gpu[gpu_num])
            self.graph_weight = [[0.0 for _ in range(left_size)] for _ in range(right_size)]
            
            for left_id, left_node in enumerate(left_node_gpu[gpu_num]):
                for right_id, right_node in enumerate(right_node_gpu[gpu_num]):
                    self.graph_weight[left_id][right_id] = left_node.calc_used_ratio(right_node)
            
            self.lx = [max(self.graph_weight[i]) for i in range(left_size)]
            self.ly = [0.0 for _ in range(right_size)]
            self.match = [-1 for _ in range(right_size)]
            self.tt = 0
            self.si = [0 for _ in range(left_size)]
            self.ti = [0 for _ in range(right_size)]
            for left_id, left_node in enumerate(left_node_gpu[gpu_num]):
                self.sla = [float("inf") for _ in range(right_size)]
                while True:
                    self.tt += 1
                    find_flag = self.find(left_id, left_size, right_size)
                    if find_flag==True:
                        break
                    else:
                        self.update(left_size, right_size)

            for right_id, right_node in enumerate(right_node_gpu[gpu_num]):
                left_node_gpu[gpu_num][self.match[right_id]].add_job(right_node)

        return left_node_gpu

    def run_jobs_to_packing(self, run_jobs):
        node = list()
        for rjob in run_jobs:
            tmp_packing = _Packing(rjob)
            node.append(tmp_packing)
        return node

    def run(self, run_jobs_list):
        self.run_jobs_list = run_jobs_list
        left_node = self.run_jobs_to_packing(self.run_jobs_list[0])
        left_node_gpu = self.KM_GPU_num(left_node)
        for i in range(1, FLAGS.multi_resource):
            right_node = self.run_jobs_to_packing(self.run_jobs_list[i])
            right_node_gpu = self.KM_GPU_num(right_node)
            new_left_node_gpu = self.KM_one_round(left_node_gpu, right_node_gpu)
            left_node_gpu = new_left_node_gpu
        return left_node_gpu

class _Matching_Same(object):
    '''
    matching algorithm for multi-resource packing
    only match jobs with the same #GPU
    '''
    def __init__(self):
        self.run_jobs_list = list()

        # params for matching
        #int
        self.match = list()
        self.tt = 0
        self.si = list()
        self.ti = list()
        #double
        self.graph_weight = list()
        self.lx = list()
        self.ly = list()
        self.sla = list()

    def KM_GPU_num(self, node):
        node_gpu = dict()
        for rnode in node:
            num_gpu = rnode.num_gpu
            if num_gpu not in node_gpu:
                node_gpu[num_gpu] = list()
            
            node_gpu[num_gpu].append(rnode)
        return node_gpu

    # find path for KM
    def find(self, idx, left_size):
        self.si[idx] = self.tt
        for i in range(left_size):
            if self.ti[i] == self.tt:
                continue
            if math.isclose(self.lx[idx]+self.ly[i], self.graph_weight[idx][i], rel_tol=1e-6):
                self.ti[i] = self.tt
                if self.match[i]==-1:
                    self.match[i] = idx
                    return True
                else:
                    find_flag = self.find(self.match[i], left_size)
                    if find_flag:
                        self.match[i] = idx
                        return True
            else:   
                self.sla[i] = min(self.sla[i], self.lx[idx]+self.ly[i]-self.graph_weight[idx][i])
        return False

    # update for KM
    def update(self, left_size):
        tmp_val = min([self.sla[i] for i in range(left_size) if self.ti[i]!=self.tt]+[float("inf")])
        for i in range(left_size):
            if self.si[i] == self.tt:
                self.lx[i] -= tmp_val
        for i in range(left_size):
            if self.ti[i] == self.tt:
                self.ly[i] += tmp_val
            else:
                self.sla[i] -= tmp_val

    # KM algorithm
    def KM_one_round(self, left_node_gpu, right_node_gpu):
        left_gpu_num = list(left_node_gpu.keys())
        right_gpu_num = list(right_node_gpu.keys())
        gpu_num_list = left_gpu_num + right_gpu_num
        gpu_num_list = list(set(gpu_num_list))
        gpu_num_list.sort(reverse=True)

        for gpu_num in gpu_num_list:
            assert gpu_num in left_gpu_num

            # print(gpu_num)
            # assert gpu_num in right_gpu_num

            # build the graph
            left_size = len(left_node_gpu[gpu_num])
            if gpu_num not in right_node_gpu:
                continue
            right_size = len(right_node_gpu[gpu_num])
            # print(left_size, right_size)
            assert left_size >= right_size
            self.graph_weight = [[0.0 for _ in range(left_size)] for _ in range(left_size)]
            
            for left_id, left_node in enumerate(left_node_gpu[gpu_num]):
                for right_id, right_node in enumerate(right_node_gpu[gpu_num]):
                    # print(left_id, left_node.num_gpu, right_id, right_node.num_gpu)
                    self.graph_weight[left_id][right_id] = left_node.calc_used_ratio(right_node)
            
            self.lx = [max(self.graph_weight[i]) for i in range(left_size)]
            self.ly = [0.0 for _ in range(left_size)]
            self.match = [-1 for _ in range(left_size)]
            self.tt = 0
            self.si = [0 for _ in range(left_size)]
            self.ti = [0 for _ in range(left_size)]
            for left_id, left_node in enumerate(left_node_gpu[gpu_num]):
                self.sla = [float("inf") for _ in range(left_size)]
                while True:
                    self.tt += 1
                    find_flag = self.find(left_id, left_size)
                    # print(left_id, find_flag)
                    if find_flag==True:
                        break
                    else:
                        self.update(left_size)

            for right_id, right_node in enumerate(right_node_gpu[gpu_num]):
                left_node_gpu[gpu_num][self.match[right_id]].add_job(right_node)

        return left_node_gpu

    def run_jobs_to_packing(self, run_jobs):
        node = list()
        for rjob in run_jobs:
            tmp_packing = _Packing(rjob)
            node.append(tmp_packing)
            # print(tmp_packing.num_gpu)
        # print("packing")
        return node

    def run(self, run_jobs_list):
        self.run_jobs_list = run_jobs_list
        left_node = self.run_jobs_to_packing(self.run_jobs_list[0])
        left_node_gpu = self.KM_GPU_num(left_node)
        for i in range(1, FLAGS.multi_resource):
            right_node = self.run_jobs_to_packing(self.run_jobs_list[i])
            right_node_gpu = self.KM_GPU_num(right_node)
            new_left_node_gpu = self.KM_one_round(left_node_gpu, right_node_gpu)
            left_node_gpu = new_left_node_gpu
        return left_node_gpu


Matching_Split = _Matching_Split()

Matching_Same = _Matching_Same()
