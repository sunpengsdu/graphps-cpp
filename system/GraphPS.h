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
                   std::vector<bool>& ActiveVector,
                   const int32_t step) {
  _Computing_Num++;
  DataPath += std::to_string(P_ID);
  DataPath += ".edge.npy";
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
  result[end_id-start_id+4] = 0; //sparsity ratio
  result[end_id-start_id+3] = (int32_t)std::floor(start_id*1.0/10000);
  result[end_id-start_id+2] = (int32_t)start_id%10000;
  result[end_id-start_id+1] = (int32_t)std::floor(end_id*1.0/10000);
  result[end_id-start_id+0] = (int32_t)end_id%10000;
  int32_t i   = 0;
  int32_t k   = 0;
  int32_t tmp = 0;
  T   rel = 0;
  int32_t changed_num = 0;
  #pragma omp parallel for num_threads(OMPNUM) private(k, tmp, rel) reduction (+:changed_num) schedule(dynamic, 1000)
  for (i=0; i < end_id-start_id; i++) {
    rel = 0;
    for (k = 0; k < indptr[i+1] - indptr[i]; k++) {
      tmp = indices[indptr[i] + k];
      rel += VertexData[tmp]/_VertexOut[tmp];
    }
    rel = rel*0.85 + 1.0/vertex_num;
    if (rel != VertexData[start_id+i]) {
      result[i] = rel - VertexData[start_id+i];
      changed_num++;
    }
  }
  clean_edge(P_ID, EdgeDataNpy);
  result[end_id-start_id+4] = (int32_t)changed_num/(end_id-start_id); //sparsity ratio
  _Computing_Num--;
  if (changed_num > 0)
    graphps_sendall<T>(std::ref(result), changed_num);
  return true;
}

template<class T>
bool comp_sssp(const int32_t P_ID,
               std::string DataPath,
               const int32_t VertexNum,
               const T* VertexData,
               const int32_t* _VertexOut,
               const int32_t* _VertexIn,
               std::vector<bool>& ActiveVector,
               const int32_t step) {
  _Computing_Num++;
  DataPath += std::to_string(P_ID);
  DataPath += ".edge.npy";
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
  result[end_id-start_id+4] = 0;
  result[end_id-start_id+3] = (int32_t)std::floor(start_id*1.0/10000);
  result[end_id-start_id+2] = (int32_t)start_id%10000;
  result[end_id-start_id+1] = (int32_t)std::floor(end_id*1.0/10000);
  result[end_id-start_id+0] = (int32_t)end_id%10000;
  // LOG(INFO) << end_id << " " << start_id;
  int32_t i   = 0;
  int32_t j   = 0;
  T   min = 0;
  int32_t changed_num = 0;
  #pragma omp parallel for num_threads(OMPNUM) private(j, min) reduction (+:changed_num) schedule(dynamic, 1000)
  for (i = 0; i < end_id-start_id; i++) {
    min = VertexData[start_id+i];
    for (j = 0; j < indptr[i+1] - indptr[i]; j++) {
      if (ActiveVector[indices[indptr[i]+j]] && min > VertexData[indices[indptr[i]+j]] + 1)
        min = VertexData[indices[indptr[i] + j]] + 1;
    }
    if (min != VertexData[start_id+i]) {
      result[i] = min - VertexData[start_id+i];
      changed_num++;
    }
  }
  clean_edge(P_ID, EdgeDataNpy);
  result[end_id-start_id+4] = (int32_t)changed_num/(end_id-start_id); //sparsity ratio
  _Computing_Num--;
  if (changed_num > 0) {
    graphps_sendall<T>(std::ref(result), changed_num);
  }
  return true;
}


template<class T>
bool comp_cc(const int32_t P_ID,
             std::string DataPath,
             const int32_t VertexNum,
             const T* VertexData,
             const int32_t* _VertexOut,
             const int32_t* _VertexIn,
             std::vector<bool>& ActiveVector,
             const int32_t step) {
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
  result[end_id-start_id+4] = 0;
  result[end_id-start_id+3] = (int32_t)std::floor(start_id*1.0/10000);
  result[end_id-start_id+2] = (int32_t)start_id%10000;
  result[end_id-start_id+1] = (int32_t)std::floor(end_id*1.0/10000);
  result[end_id-start_id+0] = (int32_t)end_id%10000;
  int32_t i   = 0;
  int32_t j   = 0;
  T   max = 0;
  int32_t changed_num = 0;
  #pragma omp parallel for num_threads(OMPNUM) private(j, max)  reduction (+:changed_num) schedule(dynamic, 1000)
  for (i = 0; i < end_id-start_id; i++) {
    max = VertexData[start_id+i];
    for (j = 0; j < indptr[i+1] - indptr[i]; j++) {
      if (max < VertexData[indices[indptr[i]+j]])
        max = VertexData[indices[indptr[i] + j]];
    }
    if (max != VertexData[start_id+i]) {
      result[i] = max - VertexData[start_id+i];
      changed_num++;
    }
  }
  clean_edge(P_ID, EdgeDataNpy);
  result[end_id-start_id+4] = (int32_t)changed_num/(end_id-start_id); //sparsity ratio
  _Computing_Num--;
  if (changed_num > 0)
    graphps_sendall<T>(std::ref(result), changed_num);
  return true;
}

template<class T>
class GraphPS {
public:
  bool (*_comp)(const int32_t,
                std::string,
                const int32_t,
                const T*,
                const int32_t*,
                const int32_t*,
                std::vector<bool>&,
                const int32_t
               ) = NULL;
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
  /////////////////
  #ifdef USE_HDFS
  LOG(INFO) << "Rank " << _my_rank << " Loading Edge From HDFS";
  start_time_hdfs();
  int hdfs_re = 0;
  hdfs_re = system("rm /home/mapred/tmp/satgraph/*");
  std::string hdfs_bin = "/opt/hadoop-1.2.1/bin/hadoop fs -get ";
  std::string hdfs_dst = "/home/mapred/tmp/satgraph/";
  #pragma omp parallel for num_threads(OMPNUM) schedule(static)
  for (int32_t k=_PartitionID_Start; k<_PartitionID_End; k++) {
    std::string hdfs_command;
    hdfs_command = hdfs_bin + _DataPath;
    hdfs_command += std::to_string(k);
    hdfs_command += ".edge.npy ";
    hdfs_command += hdfs_dst;
    hdfs_re = system(hdfs_command.c_str());
    //LOG(INFO) << hdfs_command;
  }

  LOG(INFO) << "Rank " << _my_rank << " Loading Vertex From HDFS";
  std::string hdfs_command;
  hdfs_command = hdfs_bin + _DataPath;
  hdfs_command += "vertexin.npy ";
  hdfs_command += hdfs_dst;
  hdfs_re = system(hdfs_command.c_str());
  hdfs_command.clear();
  hdfs_command = hdfs_bin + _DataPath;
  hdfs_command += "vertexout.npy ";
  hdfs_command += hdfs_dst;
  hdfs_re = system(hdfs_command.c_str());
  stop_time_hdfs();
  barrier_workers();
  if (_my_rank==0)
    LOG(INFO) << "HDFS  Load Time: " << HDFS_TIME << " ms";
  _DataPath.clear();
  _DataPath = hdfs_dst;
  #endif
  ////////////////

  init_vertex();
  std::vector<std::thread> zmq_server_pool;
  for (int32_t i=0; i<ZMQNUM; i++)
    zmq_server_pool.push_back(std::thread(graphps_server<T>, std::ref(_VertexDataNew), std::ref(_VertexData), i));
  barrier_workers();
  stop_time_init();

 if (_my_rank==0)
    LOG(INFO) << "Init Time: " << INIT_TIME << " ms";
  LOG(INFO) << "Rank " << _my_rank << " use " << _ThreadNum << " comp threads";
  std::vector<std::future<bool>> comp_pool;
  std::vector<int32_t> ActiveVector_V;
  float updated_ratio = 1.0;
  int32_t step = 0;

  // start computation
  for (step = 0; step < _MaxIteration; step++) {
    if (_my_rank==0) {
      LOG(INFO) << "Start Iteration: " << step;
    }
    start_time_comp();
#ifdef USE_ASYNC
    // memcpy(_VertexDataNew.data(), _VertexData.data(), sizeof(T)*_VertexNum);
    _VertexDataNew.assign(_VertexData.begin(), _VertexData.end());
#else
    // memset(_VertexDataNew.data(), 0, sizeof(T)*_VertexNum);
    std::fill(_VertexDataNew.begin(), _VertexDataNew.end(), 0);
#endif
    std::vector<int32_t> Partitions;
    updated_ratio = 1.0;

    for (int32_t k = _PartitionID_Start; k < _PartitionID_End; k++) {
      Partitions.push_back(k);
    }

    std::random_shuffle(Partitions.begin(), Partitions.end());
    for (int32_t &P_ID : Partitions) {
      barrier_threadpool(comp_pool, _ThreadNum*2);
      while(_Computing_Num > _ThreadNum) {
        graphps_sleep(10);
      }
      comp_pool.push_back(std::async(std::launch::async,
                                     _comp,
                                     P_ID,
                                     _DataPath,
                                     _VertexNum,
                                     _VertexData.data(),
                                     _VertexOut.data(),
                                     _VertexIn.data(),
                                     std::ref(_UpdatedLastIter),
                                     step));
    }
    barrier_threadpool(comp_pool, 0);
    barrier_workers();
    int32_t changed_num = 0;
    ActiveVector_V.clear();
    #pragma omp parallel for num_threads(_ThreadNum*OMPNUM) reduction (+:changed_num)  schedule(static)
    for (int32_t result_id = 0; result_id < _VertexNum; result_id++) {
#ifdef USE_ASYNC
      if (_VertexDataNew[result_id] == _VertexData[result_id]) {
        _UpdatedLastIter[result_id] = false;
      } else {
        _UpdatedLastIter[result_id] = true;
        changed_num += 1;
      }
#else
      _VertexData[result_id] += _VertexDataNew[result_id];
      if (_VertexDataNew[result_id] == 0) {
        _UpdatedLastIter[result_id] = false;
      } else {
        _UpdatedLastIter[result_id] = true;
        changed_num += 1;
      }
#endif
    }
    stop_time_comp();
    updated_ratio = changed_num * 1.0 / _VertexNum;
    if (_my_rank==0)
      LOG(INFO) << "Iteration: " << step
                << ", uses "<< COMP_TIME
                << " ms, Update " << changed_num 
                << ", Ratio " << updated_ratio;
    if (changed_num == 0) {
      LOG(INFO) << "Rank " << _my_rank << " Stop Running";
      break;
    }
  }
  graphps_send("!", 1, _my_rank);
  for (int32_t i=0; i<ZMQNUM; i++)
    zmq_server_pool[i].join();
}

#endif /* GRAPHPS_H_ */
