#include "../src/dataframe/distributeddataframe.h"
#include "test_util.h"
#include "../src/store/server.h"
#include <thread>
#include <functional>
#include <mutex>
#include <stdlib.h>

#define NUM_THREADS 3

void distributed_test() {
    std::thread** threads = new std::thread*[3];
    Server* s = new Server("127.0.0.1", 8080, 3);
    s->serve();
    usleep(1000000);
    for (int i = 0; i < NUM_THREADS; i++) {
        KVStore* kv = new KVStore();
        kv->configure("127.0.0.1", 33085 + i, "127.0.0.1", 8080);
        kv->setIndex(i);
        threads[i] = new std::thread(&KVStore::registerWithServer, kv);
        usleep(1000000);
    }
    KVStore* kv1 = new KVStore();
    kv1->configure("127.0.0.1", 33095, "127.0.0.1", 8080);
    kv1->registerWithServer();
    Schema* sc = new Schema("BDSI");
    DistributedDataFrame* df = new DistributedDataFrame(*sc, 4, *kv1, new String("first"));
    
    df->set(0, 0, false);
    //assert(df->get_bool(0, 0) == false);
}

int main() {
    distributed_test();
    success("DISTRIBUTED");
    return 0;
}