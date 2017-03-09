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
        const int32_t VertexNum,
        const T* VertexData,
        const int32_t* _VertexOut,
        const int32_t* _VertexIn,
        std::vector<bool>& ActiveVector) {
    _Computing_Num++;
    DataPath += std::to_string(P_ID);
    DataPath += ".edge.npy";
    // LOG(INFO) << "Processing " << DataPath;
    char* EdgeDataNpy = load_edge(P_ID, DataPath);

    int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
    int32_t start_id = EdgeData[3];
    int32_t end_id = EdgeData[4];
    int32_t indices_len = EdgeData[1];
    int32_t indptr_len = EdgeData[2];
    int32_t * indices = EdgeData + 5;
    int32_t * indptr = EdgeData + 5 + indices_len;
    int32_t vertex_num = VertexNum;
    std::vector<T> result(end_id-start_id+5, 0);
    result[end_id-start_id+4] = P_ID;
    result[end_id-start_id+3] = (int32_t)std::floor(start_id*1.0/10000);
    result[end_id-start_id+2] = (int32_t)start_id%10000;
    result[end_id-start_id+1] = (int32_t)std::floor(end_id*1.0/10000);
    result[end_id-start_id+0] = (int32_t)end_id%10000;

    // LOG(INFO) << end_id << " " << start_id;

    int32_t i   = 0;
    int32_t k   = 0;
    int32_t tmp = 0;
    T   rel = 0;
    #pragma omp parallel for num_threads(OMPNUM) private(k, tmp, rel) schedule(dynamic)
    for (i=0; i < end_id-start_id; i++) {
      rel = 0;
      for (k = 0; k < indptr[i+1] - indptr[i]; k++) {
        tmp = indices[indptr[i] + k];
        rel += VertexData[tmp]/_VertexOut[tmp];
      }
      result[i] = (rel*0.85 + 1.0/vertex_num) - VertexData[start_id+i];
      // if (std::abs(result[i] < 0.00000001))
      //    result[i] = 0;
    }
    clean_edge(P_ID, EdgeDataNpy);
    char *c_result = reinterpret_cast<char*>(&result[0]);

    _Computing_Num--;
    graphps_sendall(c_result, sizeof(T)*(end_id-start_id+5));
    return true;
}

template<class T>
bool comp_sssp(const int32_t P_ID,
        std::string DataPath,
        const int32_t VertexNum,
        const T* VertexData,
        const int32_t* _VertexOut,
        const int32_t* _VertexIn,
        std::vector<bool>& ActiveVector) {
    _Computing_Num++;
    DataPath += std::to_string(P_ID);
    DataPath += ".edge.npy";
    // LOG(INFO) << "Processing " << DataPath;
    char* EdgeDataNpy = load_edge(P_ID, DataPath);

    int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
    int32_t start_id = EdgeData[3];
    int32_t end_id = EdgeData[4];
    int32_t indices_len = EdgeData[1];
    int32_t indptr_len = EdgeData[2];
    int32_t * indices = EdgeData + 5;
    int32_t * indptr = EdgeData + 5 + indices_len;
    int32_t vertex_num = VertexNum;
    std::vector<T> result(end_id-start_id+5, 0);
    result[end_id-start_id+4] = P_ID;
    result[end_id-start_id+3] = (int32_t)std::floor(start_id*1.0/10000);
    result[end_id-start_id+2] = (int32_t)start_id%10000;
    result[end_id-start_id+1] = (int32_t)std::floor(end_id*1.0/10000);
    result[end_id-start_id+0] = (int32_t)end_id%10000;

    // LOG(INFO) << end_id << " " << start_id;
    int32_t i   = 0;
    int32_t j   = 0;
    T   min = 0;
    #pragma omp parallel for num_threads(OMPNUM) private(j, min) schedule(dynamic)
    for (i = 0; i < end_id-start_id; i++) {
        min = VertexData[start_id+i];
        for (j = 0; j < indptr[i+1] - indptr[i]; j++) {
            if (ActiveVector[indices[indptr[i]+j]] && min > VertexData[indices[indptr[i]+j]] + 1)
                min = VertexData[indices[indptr[i] + j]] + 1;
            }
        result[i] = min - VertexData[start_id+i];
    }
    clean_edge(P_ID, EdgeDataNpy);
    char *c_result = reinterpret_cast<char*>(&result[0]);

    _Computing_Num--;
    graphps_sendall(c_result, sizeof(T)*(end_id-start_id+5));
    return true;
}


template<class T>
bool comp_cc(const int32_t P_ID,
        std::string DataPath,
        const int32_t VertexNum,
        const T* VertexData,
        const int32_t* _VertexOut,
        const int32_t* _VertexIn,
        std::vector<bool>& ActiveVector) {
    _Computing_Num--;
    DataPath += ".edge.npy";
    // LOG(INFO) << "Processing " << DataPath;
    char* EdgeDataNpy = load_edge(P_ID, DataPath);
    
    int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
    int32_t start_id = EdgeData[3];
    int32_t end_id = EdgeData[4];
    int32_t indices_len = EdgeData[1];
    int32_t indptr_len = EdgeData[2];
    int32_t * indices = EdgeData + 5;
    int32_t * indptr = EdgeData + 5 + indices_len;
    int32_t vertex_num = VertexNum;
    std::vector<T> result(end_id-start_id+5, 0);
    result[end_id-start_id+4] = P_ID;
    result[end_id-start_id+3] = (int32_t)std::floor(start_id*1.0/10000);
    result[end_id-start_id+2] = (int32_t)start_id%10000;
    result[end_id-start_id+1] = (int32_t)std::floor(end_id*1.0/10000);
    result[end_id-start_id+0] = (int32_t)end_id%10000;

    // LOG(INFO) << end_id << " " << start_id;
    int32_t i   = 0;
    int32_t j   = 0;
    T   max = 0;
    #pragma omp parallel for num_threads(OMPNUM) private(j, max) schedule(dynamic)
    for (i = 0; i < end_id-start_id; i++) {
        max = VertexData[start_id+i];
        for (j = 0; j < indptr[i+1] - indptr[i]; j++) {
            //if (ActiveVector[indices[indptr[i]+j]] && max < VertexData[indices[indptr[i]+j]])
            if (max < VertexData[indices[indptr[i]+j]])
                max = VertexData[indices[indptr[i] + j]];
        }
        result[i] = max - VertexData[start_id+i];
    }
    clean_edge(P_ID, EdgeDataNpy);
    char *c_result = reinterpret_cast<char*>(&result[0]);

    _Computing_Num--;
    graphps_sendall(c_result, sizeof(T)*(end_id-start_id+5));
    return true;
}

template<class T>
class GraphPS{
public:
    bool (*_comp)(const int32_t,
            std::string,
            const int32_t,
            const T*,
            const int32_t*,
            const int32_t*,
            std::vector<bool>&) = NULL;
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
    _EdgeCache.reserve(_PartitionNum*2/_num_workers);
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
    std::thread thread_server_0(graphps_server<T>, std::ref(_VertexDataNew), 0);
    std::thread thread_server_1(graphps_server<T>, std::ref(_VertexDataNew), 1);
    barrier_workers();
    stop_time_init();
    if (_my_rank==0)
        LOG(INFO) << "Init Time: " << INIT_TIME << " ms";

    LOG(INFO) << "Rank " << _my_rank << " use " << _ThreadNum << " comp threads";
    std::vector<std::future<bool>> comp_pool;
    int32_t step = 0;
    for (step = 0; step < _MaxIteration; step++) {
        if (_my_rank==0) {
	    LOG(INFO) << "Start Iteration: " << step;
	}
        start_time_comp();
	memset(_VertexDataNew.data(), 0, sizeof(T)*_VertexNum);
        for (int32_t P_ID = _PartitionID_Start; P_ID < _PartitionID_End; P_ID++) {
       //     while(_Computing_Num > _ThreadNum) {
       //         graphps_sleep(10);
       //     }
            barrier_threadpool(comp_pool, _ThreadNum-1);
	    // LOG(INFO) << "Partition: " << P_ID;
            comp_pool.push_back(std::async(std::launch::async,
				                      _comp,
                                      P_ID,
                                      _DataPath,
                                      _VertexNum,
                                      _VertexData.data(),
                                      _VertexOut.data(),
                                      _VertexIn.data(),
                                      std::ref(_UpdatedLastIter)));
        }
        barrier_threadpool(comp_pool, 0);
        barrier_workers();

        int32_t changed_num = 0;
        #pragma omp parallel for num_threads(_ThreadNum) reduction (+:changed_num)  schedule(static)
        for (int32_t result_id = 0; result_id < _VertexNum; result_id++) {
            _VertexData[result_id] += _VertexDataNew[result_id];
            if (_VertexDataNew[result_id] == 0) {
                _UpdatedLastIter[result_id] = false;
            } else {
                _UpdatedLastIter[result_id] = true;
		changed_num += 1;
            }
        }
        stop_time_comp();
        if (_my_rank==0)
            LOG(INFO) << "Iteration: " << step
                << " uses "<< COMP_TIME
                << " ms Update " << changed_num << " Vertex";
	if (changed_num == 0)
	    break;
    }
    graphps_send("!", 1, _my_rank);
    thread_server_0.join();
    thread_server_1.join();
}

#endif /* GRAPHPS_H_ */
