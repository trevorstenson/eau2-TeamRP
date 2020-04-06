#pragma once
#include "../object.h"
#include <iostream>
#include "assert.h"

class Args : public Object {
    public:
        char* file = nullptr;
        size_t num_nodes = 0;
        size_t idx = 0;
        size_t port = 0;
        char* server_ip;
        size_t server_port = 0;
        char* app;

        Args() {}

        void parse(int argc, char** argv) {
            for (int i = 1; i < argc; i++) {
                char* a = argv[i++];
                assert( i < argc);
                char* n = argv[i];
                if (strcmp(a, "-nodes") == 0) {
                    num_nodes = atol(n);
                } else if (strcmp(a, "-index") == 0) {
                    idx = atol(n);
                } else if (strcmp(a, "-port") == 0) {
                    port = atol(n);
                } else if (strcmp(a, "-serverip") == 0) {
                    server_ip = n;
                } else if (strcmp(a, "-serverport") == 0) {
                    server_port = atol(n);
                } else {
                    assert("Unrecognized flag. Program terminated." && false);
                }
            }
        }
};