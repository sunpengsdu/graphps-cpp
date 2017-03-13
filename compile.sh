/opt/mpich/bin/mpic++  main.cpp  -O3 -m64 --force-addr  -ftree-vectorize -msse2 -ftree-vectorizer-verbose=1  -fopenmp   -L /usr/local/lib -std=c++11  -o main -lzmq -lsnappy -lz -lpthread -lglog
#/opt/mpich/bin/mpic++  main.cpp  -O3 -m64 --force-addr  -ftree-vectorize -msse2 -ftree-vectorizer-verbose=1   -L /usr/local/lib -std=c++11  -o main -lzmq -lsnappy -lpthread -lglog 
