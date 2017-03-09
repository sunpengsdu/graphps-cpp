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
        //this->_comp = comp_pagerank<T>;
        this->_comp = comp_sssp<T>;
        //this->_comp = comp_cc<T>;

    }
    void init_vertex() {
        this->load_vertex_out();
        
       //this->_VertexData.assign(this->_VertexNum, 1.0/this->_VertexNum);

       this->_VertexData.assign(this->_VertexNum, GPS_INF);
       this->_VertexData[1] = 0;
/*
        this->_VertexData.assign(this->_VertexNum, 0);
        for (int32_t i = 0; i < this->_VertexNum; i++) {
            this->_VertexData[i] = i;
        }
*/
    }
};

int main(int argc, char *argv[]) {
    start_time_app();
    FLAGS_logtostderr = 1;
    google::InitGoogleLogging(argv[0]);
    init_workers();
    //PagerankPS<double> pg;
    PagerankPS<float> pg;
    //PagerankPS<float> pg;
    // Data Path, VertexNum number, Partition number, thread number, Max Iteration
    //pg.init("/home/mapred/GraphData/eu/edge/", 1070560000, 5096, 11, 20);
    //pg.init("/home/mapred/GraphData/twitter/edge2/", 41652250, 294, 11, 20);
    //pg.init("/home/mapred/GraphData/uk/edge3/", 787803000, 2379, 11, 20);
    pg.init("/home/mapred/GraphData/webuk_3/", 133633040, 300, 11, 20);
    pg.run();
    finalize_workers();
    stop_time_app();
    LOG(INFO) << "Used " << APP_TIME/1000.0 << " s";
    return 0;
}

