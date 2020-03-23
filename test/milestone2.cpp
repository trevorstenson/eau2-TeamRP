#include "../src/application/trivial.h"

int main() {
    Trivial* t = new Trivial(0);
    t->run_();
    delete t;
    return 0;
}