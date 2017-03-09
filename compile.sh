/opt/mpich/bin/mpic++  main.cpp  -O3  -ftree-vectorize -msse2 -ftree-vectorizer-verbose=1 -fPIC -fopenmp   -L /usr/local/lib -std=c++11  -fPIC  -o main -lzmq -lsnappy -lpthread -lglog 
#/opt/openmpi/bin/mpic++  main.cpp  -O3  -ftree-vectorize -msse2 -ftree-vectorizer-verbose=1 -fPIC -fopenmp   -L /usr/local/lib -std=c++11  -fPIC  -o main -lzmq -lsnappy -lpthread -lglog 
