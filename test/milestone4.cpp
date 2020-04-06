#include "test_util.h"

#include "../src/store/server.h"
#include "../src/application/simple.h"
#include "../src/application/wordcount.h"
#include <thread>
#include <functional>
#include <mutex>
#include <stdlib.h>

#define NUM_THREADS 3

void network_test() {
    std::thread** threads = new std::thread*[3];
    Server* s = new Server("127.0.0.1", 8080, 3);
    s->serve();
    for (int i = 0; i < NUM_THREADS; i++) {
        Simple* sa = new Simple(i);
        sa->kv.configure("127.0.0.1", 33085 + i, "127.0.0.1", 8080);
        threads[i] = new std::thread(&Simple::run_, sa);
        usleep(1000000);
    }
    usleep(8000000);
    s->shutdown();
    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i]->join();
    }
}

void wordcount_test() {
    std::thread** threads = new std::thread*[3];
    Server* s = new Server("127.0.0.1", 8080, 3);
    s->serve();
    for (int i = 0; i < NUM_THREADS; i++) {
        WordCount* w = new WordCount(i);
        w->kv.configure("127.0.0.1", 33085 + i, "127.0.0.1", 8080);
        threads[i] = new std::thread(&WordCount::run_, w);
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
    //wordcount_test();
    success("M4");
    return 0;
}