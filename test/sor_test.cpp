#include "../src/sor.h"

using namespace std;

/** Test to ensure we are able to read in a .sor file */
void sor_adapter() {
    SorAdapter* sor = new SorAdapter(0, UINT32_MAX, "test1000.sor");
    DataFrame* df = sor->df_;
    //Checking the schema is correct
    assert(strcmp(df->get_schema().types, "BFSIBFSIBF") == 0);
    //Checking the data is correct
    assert(df->get_float(1, 0) == 0.01f);
    assert(df->get_float(9, 999) == 99.99f);
    assert(df->get_float(5, 570) == 57.05f);
    assert(df->get_int(3, 365) == 3653);
    assert(df->get_int(7, 723) == 7237);
    assert(df->get_bool(0, 747) == true);
    assert(df->get_bool(4, 842) == false);
    assert(df->get_string(6, 747)->equals(new String("qhmwxttofqrwxqo")));
    assert(df->get_string(2, 973)->equals(new String("pklrcomvikpjzpx")));
    //Check the row counts are all correct
    assert(df->nrows() == 1000);
    assert(df->ncols() == 10);
    cout << "---SOR adapter passed---\n";
}


int main() {
    sor_adapter();

    cout << "+++SOR adapter tests passed+++\n";
    return 0;
}