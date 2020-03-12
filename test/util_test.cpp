#include "../src/sor.h"

using namespace std;

/** Test the parsing functions */
void parse_test() {
    string int_str = string("213123");
    assert(parse_int(int_str) == 213123);
    string int_str_2 = string("00");
    assert(parse_int(int_str_2) == 0);
    string float_str = string("123.321");
    assert(parse_float(float_str) == 123.321f);
    string float_str_2 = string("34.967");
    assert(parse_float(float_str_2) == 34.967f);
    string bool_str = string("1");
    assert(parse_bool(bool_str) == true);
    string bool_str_2 = string("0");
    assert(parse_bool(bool_str_2) == false);
    cout << "---Parse passed---\n";
}

/** Tests type identification of strings and updating types, as well as type to char mappings */
void type_test() {
    //IDing of strings' types
    string int_str = string("213123");
    string int_str_2 = string("10");
    assert(get_field_type(int_str) == INT);
    assert(get_field_type(int_str_2) == INT);
    string float_str = string("123.321");
    string float_str_2 = string("34.967");
    assert(get_field_type(float_str) == FLOAT);
    assert(get_field_type(float_str_2) == FLOAT);
    string bool_str = string("1");
    string bool_str_2 = string("0");
    assert(get_field_type(bool_str) == BOOL);
    assert(get_field_type(bool_str_2) == BOOL);
    //Updating types
    assert(!should_change_type(INT, BOOL));
    assert(!should_change_type(FLOAT, BOOL));
    assert(!should_change_type(STRING, BOOL));
    assert(should_change_type(BOOL, INT));    
    assert(should_change_type(BOOL, STRING));
    assert(should_change_type(BOOL, FLOAT));
    //Mappings
    assert(map_to_char(BOOL) == 'B');
    assert(map_to_char(INT) == 'I');
    assert(map_to_char(STRING) == 'S');
    assert(map_to_char(FLOAT) == 'F');
    assert(map_to_type('B') == BOOL);
    assert(map_to_type('I') == INT);
    assert(map_to_type('S') == STRING);
    assert(map_to_type('F') == FLOAT);
    cout << "---Type passed---\n";
}
/** Tests the util trim function */
void trim_test() {
    string str = string("    \"hello\"   ");
    trim_whitespace(str);
    assert(str.compare(string("\"hello\"")) == 0);
    trim_quotes(str);
    assert(str.compare(string("hello")) == 0);

    string str2 = string("  \"123  hello 1\"  ");
    trim(str2);
    assert(str2.compare(string("123  hello 1")) == 0);
    cout << "---Trim passed---\n";
}


int main() {
    trim_test();
    parse_test();
    type_test();

    cout << "+++++++Util tests passed+++++++\n\n";
    return 0;
}