/*
 ============================================================================
 Name        : GraphPS.c
 Author      : Sun Peng
 Version     :
 Copyright   : Your copyright notice
 Description : Compute Pi in MPI C++
 ============================================================================
 */

#include "system/GraphPS.h"

using namespace std;

template<class T>
class PagerankPS : public GraphPS<T> {
public:
    PagerankPS():GraphPS<T>() {
        this->_comp = comp_pagerank<T>;
    }
    void init_vertex() {
        this->load_vertex_out();
        this->_VertexData.assign(this->_VertexNum, 1.0/this->_VertexNum);
    }
};

int main(int argc, char *argv[]) {
    FLAGS_logtostderr = 1;
    google::InitGoogleLogging(argv[0]);
    init_workers();
    PagerankPS<double> pg;
    // Data Path, VertexNum number, Partition number, thread number, Max Iteration
    //pg.init("/home/mapred/GraphData/eu/edge/", 1070560000, 5096, 10, 10);
    pg.init("/home/mapred/GraphData/twitter/edge2/", 41652250, 294, 11, 20);
    pg.run();
    finalize_workers();
    return 0;
}


//#
//#DataPath = '/home/mapred/GraphData/uk/edge3/'
//#VertexNum = 787803000
//#PartitionNum = 2379
//
//#DataPath = '/home/mapred/GraphData/soc/edge2/'
//#VertexNum = 4847571
//#PartitionNum = 14
//
//#DataPath = '/home/mapred/GraphData/twitter/edge2/'
//#VertexNum = 41652250
//#PartitionNum = 294
//
//DataPath = '/home/mapred/GraphData/webuk_3/'
//VertexNum = 133633040
//PartitionNum = 300
//
//#DataPath = '/home/mapred/GraphData/eu/edge/'
//#VertexNum = 1070560000
//#PartitionNum = 5096

