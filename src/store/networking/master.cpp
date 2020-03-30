#include <iostream>
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include "server.h"

using namespace std;

int main(int argc, char** argv) {
    if (argc > 2) {
        if (strcmp(argv[1], "-ip") == 0) {
            Server* s = new Server(argv[2], 8080, 30);
            s->serve();
            usleep(5000000);
            s->shutdown();
        }
    }
    return 0;
}