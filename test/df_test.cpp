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

int main() {
    df_sum_test();
    cout << "+++++++DataFrame tests passed+++++++\n\n";
    return 0;
}