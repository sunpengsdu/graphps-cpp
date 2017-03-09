/*
 * Global.h
 *
 *  Created on: 25 Feb 2017
 *      Author: sunshine
 */

#ifndef SYSTEM_GLOBAL_H_
#define SYSTEM_GLOBAL_H_

#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <iostream>
#include <cmath>
#include <thread>
#include <vector>
#include <map>
#include <unordered_map>
#include <chrono>
#include <future>
#include <glog/logging.h>
#include <zmq.h>
#include <snappy.h>
//#include <omp.h>
#include <sched.h>
#include "cnpy.h"

#define MASTER_RANK 0
#define HOST_LEN 20
#define ZMQ_PREFIX "tcp://*:"
#define ZMQ_PORT 15555
#define ZMQ_BUFFER 20*1024*1024
#define OMPNUM 2
//typedef int32_t VertexType;
//typedef int32_t PartitionIDType;

int  _my_rank;
int  _num_workers;
int  _hostname_len;
char _hostname[HOST_LEN];
char *_all_hostname;
void *_zmq_context;

std::chrono::steady_clock::time_point INIT_TIME_START;
std::chrono::steady_clock::time_point INIT_TIME_END;
std::chrono::steady_clock::time_point COMP_TIME_START;
std::chrono::steady_clock::time_point COMP_TIME_END;
int64_t INIT_TIME;
int64_t COMP_TIME;

cnpy::NpyArray load_edge(std::string & DataPath) {;
    cnpy::NpyArray npz = cnpy::npy_load(DataPath);
//    int32_t *data = reinterpret_cast<int32_t*>(npz.data);
    return npz;
}

inline int get_worker_id()
{
    return _my_rank;
}
inline int get_worker_num()
{
    return _num_workers;
}

void finalize_workers()
{
    LOG(INFO) << "Finalizing the application";
    delete [] (_all_hostname);
    zmq_ctx_destroy (_zmq_context);
    MPI_Finalize();
}

void barrier_workers()
{
    MPI_Barrier(MPI_COMM_WORLD);
}

void start_time_init() {
    INIT_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_init() {
    INIT_TIME_END = std::chrono::steady_clock::now();
    INIT_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
        (INIT_TIME_END-INIT_TIME_START).count();
}

void start_time_comp() {
    COMP_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_comp() {
    COMP_TIME_END = std::chrono::steady_clock::now();
    COMP_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
        (COMP_TIME_END-COMP_TIME_START).count();
}

void init_workers()
{
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
    MPI_Get_processor_name(_hostname, &_hostname_len);
    _all_hostname = new char[HOST_LEN * _num_workers];
    memset(_all_hostname, 0, HOST_LEN * _num_workers);
    MPI_Allgather(_hostname, HOST_LEN, MPI_CHAR, _all_hostname, HOST_LEN, MPI_CHAR, MPI_COMM_WORLD);
    if (_my_rank == 0) {
        LOG(INFO) << "Processors: " << _num_workers;
        for (int i = 0; i < _num_workers; i++)
            LOG(INFO) << "Rank " << i << ": " << _all_hostname + HOST_LEN*i;
    }
    _zmq_context = zmq_ctx_new ();
    barrier_workers();
}

void graphps_sleep(uint32_t ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

void barrier_threadpool(std::vector<std::future<bool>> & comp_pool, int32_t threshold) {
    while (1) {
       for(auto it = comp_pool.begin(); it!=comp_pool.end();) {
           auto status = it->wait_for(std::chrono::milliseconds(0));
           if (status == std::future_status::ready) {
               it = comp_pool.erase(it);
           }
           else {
               it++;
           }
       }
       if (comp_pool.size() > threshold) {
           graphps_sleep(50);
           continue;
       }
       else
           break;
    }
}

#endif /* SYSTEM_GLOBAL_H_ */
