#include "test_util.h"

#include "../src/dataframe/sor.h"

using namespace std;

/** Test to ensure we are able to read in a .sor file */
void sor_adapter() {
    SorAdapter* sor = new SorAdapter(0, UINT32_MAX, "test1000.sor");
    DataFrame* df = sor->df_;
    //Checking the schema is correct
    assert(strcmp(df->get_schema().types, "BDSIBDSIBD") == 0);
    //Checking the data is correct
    assert(df->get_double(1, 0) == 0.01);
    assert(df->get_double(9, 999) == 99.99);
    assert(df->get_double(5, 570) == 57.05);
    assert(df->get_int(3, 365) == 3653);
    assert(df->get_int(7, 723) == 7237);
    assert(df->get_bool(0, 747) == true);
    assert(df->get_bool(4, 842) == false);
    String* s1 = new String("qhmwxttofqrwxqo");
    String* s2 = new String("pklrcomvikpjzpx");
    assert(df->get_string(6, 747)->equals(s1));
    assert(df->get_string(2, 973)->equals(s2));
    //Check the row counts are all correct
    assert(df->nrows() == 1000);
    assert(df->ncols() == 10);
    delete sor;
    delete s1;
    delete s2;
}


int main() {
    sor_adapter();
    success("SOR");
    return 0;
}