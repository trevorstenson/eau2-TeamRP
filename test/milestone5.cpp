#include "../src/application/linus.h"
#include "test_util.h"

void linus_test() {
    Linus* l1 = new Linus(0);
    l1->run_();
}

int main() {
    linus_test();
    success("M5");
    return 0;
}