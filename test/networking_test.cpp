#include "test_util.h"

#include <stdlib.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include "../src/store/networking/server.h"
#include "../src/store/networking/node.h"
#include <thread>
#include <functional>
#include <mutex>

void network_test() {
    std::thread** threads = new std::thread*[3];
    Server* s = new Server("127.0.0.1", 8080, 3);
    s->serve();
    for (int i = 0; i < 3; i++) {
        threads[i] = new std::thread(&Node::registerWithServer, new Node("127.0.0.1", 9050 + i, "127.0.0.1", 8080));
        usleep(1000000);
    }
    usleep(10000000);
    s->shutdown();
    for (int i = 0; i < 3; i++) {
        threads[i]->join();
    }
}

int main() {
    network_test();
    cout << "SUCCESS: Network" << endl;
    return 0;
}