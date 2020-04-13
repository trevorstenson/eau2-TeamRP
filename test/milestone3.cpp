#include "test_util.h"

#include "../src/application/demo.h"
#include <thread>

#define NUM_THREADS 3

// Mocks the network layer - when the full network layer is implemented
// at the Application level, this will not be needed, as KVStore
// will have a Directory of nodes
KVStore* mockNetwork[NUM_THREADS];

// Implements communication across KVStores using a mocked network layer
// shared by mutltiple threads
void threaded_test() {
    int num_nodes = 3;
    std::thread* nodes = new std::thread[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        Demo* d = new Demo(i);
        mockNetwork[i] = &d->kv;
        d->setMockNetwork(mockNetwork);
        nodes[i] = std::thread(&Demo::run_, d);
        // Ensure no race conditions 
        std::this_thread::sleep_for (std::chrono::seconds(1));
    }

    for (int i = 0; i < NUM_THREADS; i++) 
        nodes[i].join();
}

int main() {
    threaded_test();
    success("M3");
    return 0;
}