#include <stdlib.h>
#include "../src/store/server.h"
#include "../src/application/simple.h"
#include <thread>
#include <functional>
#include <mutex>

#define NUM_THREADS 3

void network_test() {
    std::thread** threads = new std::thread*[3];
    Server* s = new Server("127.0.0.1", 8080, 3);
    s->serve();
    for (int i = 0; i < NUM_THREADS; i++) {
        Simple* sa = new Simple(i);
        sa->kv.configure("127.0.0.1", 47500 + i, "127.0.0.1", 8080);
        threads[i] = new std::thread(&Simple::run_, sa);
        usleep(1000000);
    }
    usleep(8000000);
    s->shutdown();
    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i]->join();
    }
}

int main() {
    network_test();
    return 0;
}