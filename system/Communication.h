/*
 * Communication.h
 *
 *  Created on: 25 Feb 2017
 *      Author: sunshine
 */

#ifndef SYSTEM_COMMUNICATION_H_
#define SYSTEM_COMMUNICATION_H_

#include "Global.h"
#include <ctime>


void zmq_send(const char * data, const int length, const int rank, const int id) {
  std::string dst("tcp://");
  dst += std::string(_all_hostname + rank*HOST_LEN);
  dst += ":";
  dst += std::to_string(ZMQ_PORT+id);
  void *requester = zmq_socket (_zmq_context, ZMQ_REQ);
  zmq_connect (requester, dst.c_str());
  char buffer [5];
  zmq_send (requester, data, length, 0);
  zmq_recv (requester, buffer, 5, 0);
  zmq_close (requester);
}

void graphps_send(const char * data, const int length, const int rank) {
  if (COMPRESS_CACHE_LEVEL == 0) {
    zmq_send(data, length, rank,  0);
  } else if (COMPRESS_CACHE_LEVEL == 1) {
    std::string compressed_data;
    int compressed_length = snappy::Compress(data, length, &compressed_data);
    zmq_send(compressed_data.c_str(), compressed_length, rank, 0);
  } else if (COMPRESS_CACHE_LEVEL > 1) {
    size_t compressed_length = 0;
    char* compressed_data = NULL;
    size_t buf_size = compressBound(length);
    compressed_length = buf_size;
    compressed_data = new char[buf_size];
    int compress_result = 0;
    compress_result = compress2((Bytef *)compressed_data,
                              &compressed_length,
                              (Bytef *)data,
                              length,
                              3);
    assert(compress_result == Z_OK);
    zmq_send(compressed_data, compressed_length, rank, 0);
    delete [] (compressed_data);
  } else {
    assert (1 == 0);
  }
}

template<class T>
void graphps_sendall(std::vector<T> & data_vector, int32_t changed_num) {
  int32_t length = 0;
  int32_t density = (int32_t)data_vector.back();
  char* data = NULL;
  std::vector<T> sparsedata_vector;
  int32_t changed_num_verify = 0;
  if (density < DENSITY_VALUE) {
    for (int32_t k=0; k<data_vector.size()-5; k++) {
      if (data_vector[k] != 0) {
        sparsedata_vector.push_back(k);
        sparsedata_vector.push_back(data_vector[k]);
        changed_num_verify++;
      }
    }
    assert(changed_num_verify == changed_num);
    sparsedata_vector.push_back(data_vector[data_vector.size()-5]);
    sparsedata_vector.push_back(data_vector[data_vector.size()-4]);
    sparsedata_vector.push_back(data_vector[data_vector.size()-3]);
    sparsedata_vector.push_back(data_vector[data_vector.size()-2]);
    sparsedata_vector.push_back(data_vector[data_vector.size()-1]);
    data = reinterpret_cast<char*>(&sparsedata_vector[0]);
    length = sizeof(T)*sparsedata_vector.size();
  } else {
    data = reinterpret_cast<char*>(&data_vector[0]);
    length = sizeof(T)*data_vector.size();
  }
  std::srand(std::time(0));

  if (COMPRESS_CACHE_LEVEL == 0) {
    for (int rank = 0; rank < _num_workers; rank++) {
      zmq_send(data, length, rank,  0);
    }
  } else if (COMPRESS_CACHE_LEVEL == 1) {
    std::string compressed_data;
    int compressed_length = snappy::Compress(data, length, &compressed_data);
    for (int rank = 0; rank < _num_workers; rank++) {
      zmq_send(compressed_data.c_str(), compressed_length, rank, 0);
    }
  } else if (COMPRESS_CACHE_LEVEL > 1) {
    size_t compressed_length = 0;
    char* compressed_data = NULL;
    size_t buf_size = compressBound(length);
    compressed_length = buf_size;
    compressed_data = new char[buf_size];
    int compress_result = 0;
    compress_result = compress2((Bytef *)compressed_data,
                              &compressed_length,
                              (Bytef *)data,
                              length,
                              3);
    assert(compress_result == Z_OK);
    for (int rank = 0; rank < _num_workers; rank++) {
      zmq_send(compressed_data, compressed_length, rank, 0);
    }
    delete [] (compressed_data);
  } else {
    assert (1 == 0);
  }
}

template<class T>
void graphps_server(std::vector<T>& VertexDataNew, std::vector<T>& VertexData, int32_t id) {
  //  Socket to talk to clients
  // std::string server_addr(ZMQ_PREFIX);
  // server_addr += std::to_string(ZMQ_PORT+_my_rank);
  // server_addr += std::to_string(ZMQ_PORT+id);
  // LOG(INFO) << "Rank " << _my_rank << " Setup ZMQ Server " << server_addr;
  void *responder = zmq_socket (_zmq_context, ZMQ_REP);
  int rc = zmq_connect (responder, "inproc://graphps_servers");
  assert (rc == 0);
  char *buffer = new char[ZMQ_BUFFER];
  char *uncompressed_c = new char[ZMQ_BUFFER];
  while (1) {
    memset(buffer, 0, ZMQ_BUFFER);
    int length = zmq_recv (responder, buffer, ZMQ_BUFFER, 0);
    std::string uncompressed;
    if (COMPRESS_CACHE_LEVEL == 0) {
      uncompressed.assign(buffer, length);
    } else if (COMPRESS_CACHE_LEVEL == 1) {
      assert (snappy::Uncompress(buffer, length, &uncompressed) == true);
    } else if (COMPRESS_CACHE_LEVEL > 1) {
      int uncompress_result = 0;
      size_t uncompressed_length = ZMQ_BUFFER;
      uncompress_result = uncompress((Bytef *)uncompressed_c,
                                    &uncompressed_length,
                                    (Bytef *)buffer,
                                    length);
      assert (uncompress_result == Z_OK);
      uncompressed.assign(uncompressed_c, uncompressed_length);
    } else {
      assert (1 == 0);
    }
    T* raw_data = (T*) uncompressed.c_str();
    int32_t raw_data_len = (uncompressed.size()) / sizeof(T);
    int32_t density = raw_data[raw_data_len-1];
    int32_t start_id = (int32_t)raw_data[raw_data_len-2]*10000 + (int32_t)raw_data[raw_data_len-3];
    int32_t end_id = (int32_t)raw_data[raw_data_len-4]*10000 + (int32_t)raw_data[raw_data_len-5];
    if (density >= DENSITY_VALUE) {
      assert(end_id-start_id == raw_data_len-5);
#ifdef USE_ASYNC
      for (int32_t k=0; k<(end_id-start_id); k++) {
        VertexData[k+start_id] += raw_data[k];
      }
#else
      for (int32_t k=0; k<(end_id-start_id); k++) {
        VertexDataNew[k+start_id] = raw_data[k];
      }
#endif
    } else {
      for (int32_t k=0; k<(raw_data_len-5); k=k+2) {
#ifdef USE_ASYNC
        VertexData[raw_data[k]+start_id] += raw_data[k+1];
#else
        VertexDataNew[raw_data[k]+start_id] = raw_data[k+1];
#endif
      }
    }
    zmq_send (responder, "ACK", 3, 0);
  }
}

#endif /* SYSTEM_COMMUNICATION_H_ */
