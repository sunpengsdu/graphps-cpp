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

//to do design the port
void graphps_send(std::string &data, const int length, const int rank) {
#ifdef SNAPPY_COMPRESS
  std::string compressed_data;
  int compressed_length = snappy::Compress(data.c_str(), length, &compressed_data);
  if (length==1 && data[0] =='!') {
    for (int32_t i = 0; i < ZMQNUM; i++)
      zmq_send(compressed_data.c_str(), compressed_length, rank, i);
  } else {
    std::srand(std::time(0));
    zmq_send(compressed_data.c_str(), compressed_length, rank, (_my_rank+std::rand())%ZMQNUM);
  }
#else
  if (length==1 && data[0] =='!') {
    for (int32_t i = 0; i < ZMQNUM; i++)
      zmq_send(data.c_str(), length, rank, i);
  } else {
    std::srand(std::time(0));
    zmq_send(data.c_str(), length, rank,  (_my_rank+std::rand())%ZMQNUM);
  }
#endif
}

void graphps_send(const char * data, const int length, const int rank) {
#ifdef SNAPPY_COMPRESS
  std::string compressed_data;
  int compressed_length = snappy::Compress(data, length, &compressed_data);
  if (length==1 && *data == '!') {
    for (int32_t i = 0; i < ZMQNUM; i++)
      zmq_send(compressed_data.c_str(), compressed_length, rank, i);
  } else {
    std::srand(std::time(0));
    zmq_send(compressed_data.c_str(), compressed_length, rank,  (_my_rank+std::rand())%ZMQNUM);
  }
#else
  if (length==1 && *data == '!') {
    for (int32_t i = 0; i < ZMQNUM; i++)
      zmq_send(data, length, rank, i);
  } else {
    std::srand(std::time(0));
    zmq_send(data, length, rank,  (_my_rank+std::rand())%ZMQNUM);
  }
#endif
}

void graphps_sendall(std::string &data, const int length) {
  std::srand(std::time(0));
#ifdef SNAPPY_COMPRESS
  std::string compressed_data;
  int compressed_length = snappy::Compress(data.c_str(), length, &compressed_data);
  //#pragma omp parallel for num_threads(OMPNUM) schedule(static)
  for (int rank = 0; rank < _num_workers; rank++)
    zmq_send(compressed_data.c_str(), compressed_length, (rank+_my_rank)%_num_workers,  (_my_rank+std::rand())%ZMQNUM);
#else
  //#pragma omp parallel for num_threads(OMPNUM) schedule(static)
  for (int rank = 0; rank < _num_workers; rank++)
    zmq_send(data.c_str(), length, (rank+_my_rank)%_num_workers,  (_my_rank+std::rand())%ZMQNUM);
#endif
}

void graphps_sendall(const char * data, const int length) {
  std::srand(std::time(0));
#ifdef SNAPPY_COMPRESS
  std::string compressed_data;
  int compressed_length = snappy::Compress(data, length, &compressed_data);
  //#pragma omp parallel for num_threads(OMPNUM) schedule(static)
  for (int rank = 0; rank < _num_workers; rank++)
    zmq_send(compressed_data.c_str(), compressed_length, (rank+_my_rank)%_num_workers,  (_my_rank+std::rand())%ZMQNUM);
#else
  //#pragma omp parallel for num_threads(OMPNUM) schedule(static)
  for (int rank = 0; rank < _num_workers; rank++)
    zmq_send(data, length,  (rank+_my_rank)%_num_workers, (_my_rank+std::rand())%ZMQNUM);
#endif
}


template<class T>
void graphps_server(std::vector<T>& VertexDataNew, int32_t id) {
  //  Socket to talk to clients
  std::string server_addr(ZMQ_PREFIX);
  // server_addr += std::to_string(ZMQ_PORT+_my_rank);
  server_addr += std::to_string(ZMQ_PORT+id);
  // LOG(INFO) << "Rank " << _my_rank << " Setup ZMQ Server " << server_addr;
  void *responder = zmq_socket (_zmq_context, ZMQ_REP);
  int rc = zmq_bind (responder, server_addr.c_str());
  assert (rc == 0);
  char *buffer = new char[ZMQ_BUFFER];
  while (1) {
    memset(buffer, 0, ZMQ_BUFFER);
    int length = zmq_recv (responder, buffer, ZMQ_BUFFER, 0);
    std::string uncompressed;
#ifdef SNAPPY_COMPRESS
    assert (snappy::Uncompress(buffer, length, &uncompressed) == true);
#else
    uncompressed.assign(buffer, length);
#endif
    if (uncompressed.length() == 1 and uncompressed == "!") {
      zmq_send (responder, "ACK", 3, 0);
      //LOG(INFO) << "Existing the graphps_server";
      zmq_close(responder);
      break;
    } else {
      T* raw_data = (T*) uncompressed.c_str();
#ifdef SNAPPY_COMPRESS
      int32_t raw_data_len = (uncompressed.size()) / sizeof(T);
#else
      int32_t raw_data_len = length / sizeof(T);
#endif
      int32_t partition_id = raw_data[raw_data_len-1];
      int32_t start_id = (int32_t)raw_data[raw_data_len-2]*10000 + (int32_t)raw_data[raw_data_len-3];
      int32_t end_id = (int32_t)raw_data[raw_data_len-4]*10000 + (int32_t)raw_data[raw_data_len-5];
      assert(end_id-start_id == raw_data_len-5);
      memcpy(VertexDataNew.data()+start_id, raw_data, sizeof(T)*(end_id-start_id));
      zmq_send (responder, "ACK", 3, 0);
    }
  }
}

#endif /* SYSTEM_COMMUNICATION_H_ */
