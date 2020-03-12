#include "../sor.h"

using namespace std;

void sor_helpers() {

    SorAdapter* sor = new SorAdapter(0, UINT16_MAX, "test1000.sor");

    cout << "---Sor helpers passed---\n";
}


int main() {
    sor_helpers();

    return 0;
}