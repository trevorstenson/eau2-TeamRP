#include "../src/sor.h"

using namespace std;

/** Tests the util trim function */
void trim_test() {
    string str = string("    \"hello\"   ");
    trimWhitespace(str);
    assert(str.compare(string("\"hello\"")) == 0);
    trimQuotes(str);
    assert(str.compare(string("hello")) == 0);

    string str2 = string("  \"123  hello 1\"  ");
    trim(str2);
    assert(str2.compare(string("123  hello 1")) == 0);
    cout << "---Trim passed---\n";
}


int main() {
    trim_test();

    cout << "+++Util tests passed+++\n";
    return 0;
}