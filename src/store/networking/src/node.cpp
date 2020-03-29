#include <iostream>
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include "node.h"

using namespace std;

int main(int argc, char** argv) {
    if (argc > 4) {
        if (strcmp(argv[1], "-ip") == 0 && strcmp(argv[3], "-port") == 0) {
            Node* n1 = new Node(argv[2], atoi(argv[4]), "127.0.0.1", 8080);
            n1->registerWithServer();
            delete n1;
        }
    }
    return 0;
}