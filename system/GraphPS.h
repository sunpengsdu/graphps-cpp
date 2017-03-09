/*
 * GraphPS.h
 *
 *  Created on: 25 Feb 2017
 *      Author: sunshine
 */

#ifndef GRAPHPS_H_
#define GRAPHPS_H_

#include "Global.h"
#include "Communication.h"

template<class T>
bool comp_pagerank(const int32_t P_ID,
        std::string DataPath,
        std::vector<T>& VertexData,
        std::vector<int32_t>& _VertexOut,
        std::vector<int32_t>& _VertexIn) {
    //    omp_set_dynamic(0);
    //    omp_set_num_threads(OMPNUM);

    DataPath += std::to_string(P_ID);
    DataPath += ".edge.npy";
    LOG(INFO) << "Processing " << DataPath;

    cnpy::NpyArray EdgeDataNpy = load_edge(DataPath);
    int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy.data);
    int32_t start_id = EdgeData[3];
    int32_t end_id = EdgeData[4];
    int32_t indices_len = EdgeData[1];
    int32_t indptr_len = EdgeData[2];
    int32_t * indices = EdgeData + 5;
    int32_t * indptr = EdgeData + 5 + indices_len;
    int32_t vertex_num = _VertexOut.size();
    std::vector<T> result(end_id-start_id+5, 0);
    result[end_id-start_id+4] = P_ID;
    result[end_id-start_id+3] = std::floor(start_id*1.0/10000);
    result[end_id-start_id+2] = start_id%10000;
    result[end_id-start_id+1] = std::floor(end_id*1.0/10000);
    result[end_id-start_id+0] = end_id%10000;

    int32_t i   = 0;
    int32_t k   = 0;
    int32_t tmp = 0;
    float   rel = 0;
//    omp_set_dynamic(0);
//    omp_set_num_threads(OMPNUM);
//    #pragma omp parallel for private(k, tmp, rel) schedule(dynamic)
    for (i=0; i < end_id-start_id; i++) {
      rel = 0;
      for (k = 0; k < indptr[i+1] - indptr[i]; k++) {
        tmp = indices[indptr[i] + k];
        rel += VertexData[tmp]/_VertexOut[tmp];
      }
      result[i] = rel*0.85 + 1.0/vertex_num - VertexData[start_id+i];
    }

    EdgeDataNpy.destruct();
    char *c_result = reinterpret_cast<char*>(&result[0]);
    graphps_sendall(c_result, sizeof(T)*(end_id-start_id+5));
    return true;
}

template<class T>
class GraphPS{
public:
    bool (*_comp)(const int32_t,
            std::string,
            std::vector<T>&,
            std::vector<int32_t>&,
            std::vector<int32_t>&) = NULL;
    T _FilterThreshold;
    std::string _DataPath;
    std::string _Scheduler;
    int32_t _ThreadNum;
    int32_t _VertexNum;
    int32_t _PartitionNum;
    int32_t _MaxIteration;
    int32_t _PartitionID_Start;
    int32_t _PartitionID_End;
    std::map<int, std::string> _AllHosts;
    std::map<int32_t, cnpy::NpyArray> _EdgeCache;
    std::vector<int32_t> _VertexOut;
    std::vector<int32_t> _VertexIn;
    std::vector<T> _VertexData;
    std::vector<T> _VertexDataNew;
    std::vector<bool> _UpdatedLastIter;
    GraphPS();
    void init(std::string DataPath,
            const int32_t VertexNum,
            const int32_t PartitionNum,
            const int32_t ThreadNum=4,
            const int32_t MaxIteration=10);

//    virtual void compute(const int32_t PartitionID)=0;
    virtual void init_vertex()=0;
    void set_threadnum (const int32_t ThreadNum);
    void run();
    void load_vertex_in();
    void load_vertex_out();
};

template<class T>
GraphPS<T>::GraphPS() {
    _VertexNum = 0;
    _PartitionNum = 0;
    _MaxIteration = 0;
    _ThreadNum = 1;
    _PartitionID_Start = 0;
    _PartitionID_End = 0;
}

template<class T>
void GraphPS<T>::init(std::string DataPath,
                const int32_t VertexNum,
                const int32_t PartitionNum,
                const int32_t ThreadNum,
                const int32_t MaxIteration) {
    start_time_init();
    _ThreadNum = ThreadNum;
    _DataPath = DataPath;
    _VertexNum = VertexNum;
    _PartitionNum = PartitionNum;
    _MaxIteration = MaxIteration;
    for (int i = 0; i < _num_workers; i++) {
        std::string host_name(_all_hostname + i * HOST_LEN);
        _AllHosts[i] = host_name;
    }

    _Scheduler = _AllHosts[0];
    _UpdatedLastIter.assign(_VertexNum, true);
    _VertexDataNew.assign(_VertexNum, 0);
    init_vertex();

    int32_t n = std::ceil(_PartitionNum*1.0/_num_workers);
    _PartitionID_Start = (_my_rank*n < _PartitionNum) ? _my_rank*n:-1;
    _PartitionID_End = ((1+_my_rank)*n > _PartitionNum) ? _PartitionNum:(1+_my_rank)*n;
    LOG(INFO) << "Rank " << _my_rank << " "
            << " With Partitions From " << _PartitionID_Start << " To " << _PartitionID_End;
}

template<class T>
void  GraphPS<T>::load_vertex_in() {
    std::string vin_path = _DataPath + "vertexin.npy";
    cnpy::NpyArray npz = cnpy::npy_load(vin_path);
    int32_t *data = reinterpret_cast<int32_t*>(npz.data);
    _VertexIn.assign(data, data+_VertexNum);
    npz.destruct();
}

template<class T>
void  GraphPS<T>::load_vertex_out() {
    std::string vout_path = _DataPath + "vertexout.npy";
    cnpy::NpyArray npz = cnpy::npy_load(vout_path);
    int32_t *data = reinterpret_cast<int32_t*>(npz.data);
    _VertexOut.assign(data, data+_VertexNum);
    npz.destruct();
}

template<class T>
void GraphPS<T>::run() {
//    omp_set_dynamic(0);
//    omp_set_num_threads(OMPNUM);
    std::thread thread_server(graphps_server<T>, std::ref(_VertexDataNew));
    barrier_workers();
    stop_time_init();
    if (_my_rank==0)
        LOG(INFO) << "Init Time: " << INIT_TIME << " ms";

    std::vector<std::future<bool>> comp_pool;
    int32_t step = 0;
    for (step = 0; step < _MaxIteration; step++) {
        start_time_comp();
        for (int32_t P_ID = _PartitionID_Start; P_ID < _PartitionID_End; P_ID++) {
            barrier_threadpool(comp_pool, _ThreadNum-1);
            comp_pool.push_back(std::async(_comp,
                                P_ID,
                                _DataPath,
                                std::ref(_VertexData),
                                std::ref(_VertexOut),
                                std::ref(_VertexIn)));
        }
        barrier_threadpool(comp_pool ,0);
        barrier_workers();

//        #pragma omp parallel for schedule(static)
        for (int32_t result_id = 0; result_id < _VertexNum; result_id++) {
            _VertexData[result_id] -= _VertexDataNew[result_id];
            if (_VertexDataNew[result_id] == 0) {
                _UpdatedLastIter[result_id] = true;
            } else {
                _UpdatedLastIter[result_id] = false;
            }
        }
        int32_t changed_num = 0;
        for (auto t_it = _UpdatedLastIter.begin(); t_it != _UpdatedLastIter.end(); t_it++) {
            changed_num += *t_it;
        }
        stop_time_comp();
        if (_my_rank==0)
            LOG(INFO) << "Iteration: " << step
                << " uses "<< COMP_TIME
                << " ms Updateed " << changed_num << " Vertex";
    }
    graphps_send("!", 1, _my_rank);
    thread_server.join();
}

#endif /* GRAPHPS_H_ */
