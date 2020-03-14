#include <iostream>
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include "dataframe.h"
#include "sor.h"
#include "limits.h"

void trivial_test() {
    SorAdapter* s100000 = new SorAdapter(0, UINT_MAX, "test1000.sor");
    s100000->get_df()->print();
    delete s100000;    
}

void schema_test() {
    Schema* s = new Schema();
    String* s1 = new String("Hello");
    s->add_column('B', s1);
    s->add_row(s1);
    delete s;
    delete s1;
}

void column_test() {
    Column* c = new IntColumn(3, 56, 2, 4);
    c->as_int()->set(1, 4);
    c->as_int()->push_back(9);
    c->as_int()->push_back(345);
    c->print();
    delete c;
}

int main() {
    trivial_test();
    //column_test();
    //schema_test();

    return 0;
}