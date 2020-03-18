#include "../src/application/trivial.h"

int main() {
    Trivial* t = new Trivial(0);

    delete t;
    return 0;
}