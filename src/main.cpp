#include <iostream>
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include "dataframe.h"
#include "sor.h"
#include "limits.h"

void trivial_test() {
    SorAdapter* s100000 = new SorAdapter(0, UINT_MAX, "test100000.sor");
    s100000->get_df()->print();
}

int main() {
    trivial_test();

    return 0;
}