#include "../src/sor.h"
#include "../src/rowers.h"

using namespace std;

void df_sum_test() {
    SorAdapter* sor = new SorAdapter(0, UINT32_MAX, "test1000.sor");
    DataFrame* df = sor->get_df();
    SumNumbers* sn = new SumNumbers();
    df->map(*sn);
    cout << "Total sum: " << sn->get_sum() << endl;
    delete sn;
    delete sor;
    cout << "---SumNumbers passed---" << endl;
}

void df_equals_test() {
    Schema* schema = new Schema("BDSI");
    Schema* schema2 = new Schema("BDSI");
    DataFrame* df = new DataFrame(*schema);
    DataFrame* df2 = new DataFrame(*schema2);
    assert(df->equals(df2));
    assert(!df->equals(new String("Hello")));
    df->set(1, 0, 100.1);
    assert(!df->equals(df2));
    df2->set(1, 0, 100.1);
    assert(df->equals(df2));
    df2->set(1, 1, 100.2);
    df->set(1, 1, 100.1);
    assert(!df->equals(df2));
    cout << "---DF Equality passed---" << endl;
}

/** Makes sure we can read in SOR, deserialize, and reserialzie */
void integration_test() {
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
    assert(df->get_string(6, 747)->equals(new String("qhmwxttofqrwxqo")));
    assert(df->get_string(2, 973)->equals(new String("pklrcomvikpjzpx")));
    //Check the row counts are all correct
    assert(df->nrows() == 1000);
    assert(df->ncols() == 10);
    unsigned char* serial = df->serialize();
    df->debug_print();
    DataFrame* df2 = new DataFrame(serial);
    df2->debug_print();
    assert(df->equals(df2));

    cout << "---SOR - DataFrame - Serialization Integration passed---" << endl;
}

int main() {
    df_sum_test();
    df_equals_test();
    integration_test();
    cout << "+++++++DataFrame tests passed+++++++\n\n";
    return 0;
}