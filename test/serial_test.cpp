#include "test_util.h"

#include "../src/serial/array.h"
#include "../src/serial/message.h"
#include "../src/store/key.h"
#include "../src/store/value.h"
#include "../src/serial/serial.h"
#include "../src/dataframe/dataframe.h"

using namespace std;

void size_t_test() {
    size_t a = 81;
    unsigned char* a_serial = serialize_size_t(a);
    size_t a_deserial = deserialize_size_t(a_serial);
    assert(a == a_deserial);
    delete[] a_serial;

    size_t b = 578349013;
    unsigned char* b_serial = serialize_size_t(b);
    size_t b_deserial = deserialize_size_t(b_serial);
    assert(b == b_deserial);
    delete[] b_serial;

    size_t c = 12301;
    unsigned char* c_serial = serialize_size_t(c);
    size_t c_deserial = deserialize_size_t(c_serial);
    assert(c == c_deserial);
    delete[] c_serial;

    size_t d = 10000000000000;
    unsigned char* d_serial = serialize_size_t(d);
    size_t d_deserial = deserialize_size_t(d_serial);
    assert(d == d_deserial);
    delete[] d_serial;
}

void double_test() {
    double a = 421.3213;
    unsigned char* a_serial = serialize_double(a);
    double a_deserial = deserialize_double(a_serial);
    assert(a == a_deserial);
    delete[] a_serial;

    double b = 0.24124;
    unsigned char* b_serial = serialize_double(b);
    double b_deserial = deserialize_double(b_serial);
    assert(b == b_deserial);
    delete[] b_serial;

    double c = 213343241.123;
    unsigned char* c_serial = serialize_double(c);
    double c_deserial = deserialize_double(c_serial);
    assert(c == c_deserial);
    delete[] c_serial;

    double d = 1032136950.4326217;
    unsigned char* d_serial = serialize_double(d);
    double d_deserial = deserialize_double(d_serial);
    assert(d == d_deserial);
    delete[] d_serial;
}

void string_test() {
    String* a = new String("Hello");
    unsigned char* a_serial = serialize_string(a);
    String* a_deserial = deserialize_string(a_serial);
    assert(a->equals(a_deserial));
    delete a;
    delete[] a_serial;
    delete a_deserial;

    String* b = new String("&cwd189a&%^C&WQ");
    unsigned char* b_serial = serialize_string(b);
    String* b_deserial = deserialize_string(b_serial);
    assert(b->equals(b_deserial));
    delete b;
    delete[] b_serial;
    delete b_deserial;

    String* c = new String("");
    unsigned char* c_serial = serialize_string(c);
    String* c_deserial = deserialize_string(c_serial);
    assert(c->equals(c_deserial));
    delete c;
    delete[] c_serial;
    delete c_deserial;
}

void double_array_test() {
    DoubleArray* a = new DoubleArray();
    a->push(53441.43);
    a->push(95746.94);
    a->push(0.56478901);
    a->push(542.45);
    a->push(325.342);
    unsigned char* a_serial = a->serialize();
    DoubleArray* a_deserial = new DoubleArray();
    assert(!a->equals(a_deserial));
    a_deserial->deserialize(a_serial);
    assert(a->equals(a_deserial));
    a->push(542.4125);
    assert(!a->equals(a_deserial));
    a_deserial->push(542.4125);
    assert(a->equals(a_deserial));
    delete a;
    delete[] a_serial;
    delete a_deserial;
}

void string_array_test() {
    StringArray* a = new StringArray(50);
    for (size_t i = 0; i < 10; i++) {
        a->push(new String("hello"));
        a->push(new String("excellent"));
        a->push(new String("string3"));
        a->push(new String("dog"));
        a->push(new String("cat"));
    }
    unsigned char* a_serial = a->serialize();
    StringArray* a_deserial = new StringArray(10);
    assert(!a->equals(a_deserial));
    a_deserial->deserialize(a_serial);
    assert(a->equals(a_deserial));
    delete a;
    delete[] a_serial;
    delete a_deserial;
}

void directory_test() {
    Directory* d = new Directory();
    d->kind_ = MsgKind::Directory;  // the message kind
    d->sender_ = 1234; // the index of the sender node
    d->target_ = 9999; // the index of the receiver node
    d->id_ = 1111;
    d->client = 123456;
    d->push_port(12121212);
    d->push_port(34343434);
    d->addresses->push(new String("1.2.3.4"));
    d->addresses->push(new String("5.6.7.8"));
    unsigned char* d_serial = d->serialize();
    Directory* d_deserial = new Directory(d_serial);
    assert(d->equals(d_deserial));
    d->push_port(12121213);
    assert(!d->equals(d_deserial));
    d_deserial->push_port(12121213);
    assert(d->equals(d_deserial));
    d->client = 11;
    assert(!d->equals(d_deserial));
    d->client = 123456;
    assert(d->equals(d_deserial));
    d_deserial->addresses->push(new String("bad"));
    assert(!d->equals(d_deserial));
    delete d;
    delete[] d_serial;
    delete d_deserial;
}

void ack_test() {
    Ack* message = new Ack();
    message->kind_ = MsgKind::Ack;  // the message kind
    message->sender_ = 1234; // the index of the sender node
    message->target_ = 9999; // the index of the receiver node
    message->id_ = 1111;
    message->previous_kind = MsgKind::Directory;
    unsigned char* message_serialized = message->serialize();
    Ack* message_deserialized = new Ack(message_serialized);
    assert(message->equals(message_deserialized));
    message->sender_ = 123;
    assert(!message->equals(message_deserialized));
    message->sender_ = 1234;
    assert(message->equals(message_deserialized));
    message->target_ = 123;
    assert(!message->equals(message_deserialized));
    message->target_ = 9999;
    assert(message->equals(message_deserialized));
    message->id_ = 123;
    assert(!message->equals(message_deserialized));
    message->id_ = 1111;
    assert(message->equals(message_deserialized));
    message->previous_kind = MsgKind::Reply;
    assert(!message->equals(message_deserialized));
    delete message;
    delete[] message_serialized;
    delete message_deserialized;
}

void nack_test() {
    Nack* message = new Nack();
    message->kind_ = MsgKind::Nack;  // the message kind
    message->sender_ = 1234; // the index of the sender node
    message->target_ = 9999; // the index of the receiver node
    message->id_ = 1111;
    message->previous_kind = MsgKind::Directory;
    unsigned char* message_serialized = message->serialize();
    Nack* message_deserialized = new Nack(message_serialized);
    assert(message->equals(message_deserialized));
    message->sender_ = 123;
    assert(!message->equals(message_deserialized));
    message->sender_ = 1234;
    assert(message->equals(message_deserialized));
    message->target_ = 123;
    assert(!message->equals(message_deserialized));
    message->target_ = 9999;
    assert(message->equals(message_deserialized));
    message->id_ = 123;
    assert(!message->equals(message_deserialized));
    message->id_ = 1111;
    assert(message->equals(message_deserialized));
    message->previous_kind = MsgKind::Reply;
    assert(!message->equals(message_deserialized));
    delete message;
    delete[] message_serialized;
    delete message_deserialized;
}

void status_test() {
    Status* message = new Status();
    message->kind_ = MsgKind::Status;  // the message kind
    message->sender_ = 1234; // the index of the sender node
    message->target_ = 9999; // the index of the receiver node
    message->id_ = 1111;
    message->msg_ = new String("test123");
    unsigned char* message_serialized = message->serialize();
    Status* message_deserialized = new Status(message_serialized);
    assert(message->equals(message_deserialized));
    message->sender_ = 123;
    assert(!message->equals(message_deserialized));
    message->sender_ = 1234;
    assert(message->equals(message_deserialized));
    message->target_ = 123;
    assert(!message->equals(message_deserialized));
    message->target_ = 9999;
    assert(message->equals(message_deserialized));
    message->id_ = 123;
    assert(!message->equals(message_deserialized));
    message->id_ = 1111;
    assert(message->equals(message_deserialized));
    message->msg_ = new String("test1234");
    delete message;
    delete[] message_serialized;
    delete message_deserialized;
}

void register_test() {
    Register* message = new Register();
    message->kind_ = MsgKind::Register;  // the message kind
    message->sender_ = 1234; // the index of the sender node
    message->target_ = 9999; // the index of the receiver node
    message->id_ = 1111;
    message->port = 12332;
    message->IP = new String("test123");
    unsigned char* message_serialized = message->serialize();
    Register* message_deserialized = new Register(message_serialized);
    assert(message->equals(message_deserialized));
    message->sender_ = 123;
    assert(!message->equals(message_deserialized));
    message->sender_ = 1234;
    assert(message->equals(message_deserialized));
    message->target_ = 123;
    assert(!message->equals(message_deserialized));
    message->target_ = 9999;
    assert(message->equals(message_deserialized));
    message->id_ = 123;
    assert(!message->equals(message_deserialized));
    message->id_ = 1111;
    assert(message->equals(message_deserialized));
    message->IP = new String("test1234");
    assert(!message->equals(message_deserialized));
    message->IP = new String("test123");
    assert(message->equals(message_deserialized));
    message->port = 123321;
    assert(!message->equals(message_deserialized));
    delete message;
    delete[] message_serialized;
    delete message_deserialized;
}

void network_utility() {
    Ack* ack = new Ack();
    ack->kind_ = MsgKind::Ack;
    unsigned char* ack2 = ack->serialize();
    assert(MsgKind::Ack == message_kind(ack2));
    assert(34 == message_length(ack2));
    delete ack;
    delete[] ack2;

    Nack* nack = new Nack();
    nack->kind_ = MsgKind::Nack;
    unsigned char* nack2 = nack->serialize();
    assert(MsgKind::Nack == message_kind(nack2));
    assert(34 == message_length(ack2));
    delete nack;
    delete[] nack2;

    Status* status = new Status();
    status->kind_ = MsgKind::Status;
    status->msg_ = new String("hello");
    unsigned char* status2 = status->serialize();
    assert(MsgKind::Status == message_kind(status2));
    assert(39 == message_length(status2));
    delete status;
    delete[] status2;
}

void kill_test() {
    Kill* kill = new Kill();
    unsigned char* kill2 = kill->serialize();
    assert(MsgKind::Kill == message_kind(kill2));
    assert(33 == message_length(kill2));
    Kill* kill_d = new Kill(kill2);
    assert(kill->equals(kill_d));
    delete kill;
    delete[] kill2;
    delete kill_d;
}

void schema_test() {
    Schema* schema = new Schema("BDBSISDBSDI");
    schema->new_length(18);
    unsigned char* serial = schema->serialize();
    Schema* schema2 = new Schema(serial);
    assert(schema->equals(schema2));
    delete schema;
    delete[] serial;
    delete schema2;
}

void df_test() {
    Schema* schema = new Schema("BDIS");
    DataFrame* df = new DataFrame(*schema);
    df->set(0, 0, true);
    df->set(1, 0, 31.5);
    df->set(2, 0, 54);
    df->set(3, 0, new String("Hello"));
    df->set(0, 1, true);
    df->set(1, 1, 31231.5);
    df->set(2, 1, 514);
    df->set(3, 1, new String("Hello1"));
    df->set(0, 2, false);
    df->set(1, 2, 37.5);
    df->set(2, 2, 5401);
    df->set(3, 2, new String("Hello2"));
    df->set(0, 3, false);
    df->set(1, 3, 22.2);
    unsigned char* serial = df->serialize();
    DataFrame* df2 = new DataFrame(serial);
    //assert(df2->equals(df));
    delete schema;
    delete df;
    delete[] serial;
    delete df2;
}

void key_test() {
    Key* key = new Key("key1", 100);
    unsigned char* serial = key->serialize();
    Key* key2 = new Key(serial);
    assert(key->equals(key2));
    Key* key3 = new Key("r3y2f9780h43vf43c892qruj4qxdf230q9d", 1232123);
    unsigned char* serial2 = key3->serialize();
    Key* key4 = new Key(serial2);
    assert(key3->equals(key4));
    assert(!key->equals(key3));
    delete key;
    delete[] serial;
    delete key2;
    delete key3;
    delete[] serial2;
    delete key4;
}

void value_test() {
    Value* value = new Value((unsigned char*)"8fjsd&@Dhv;[]&v", 15);
    unsigned char* serial = value->serialize();
    Value* value2 = new Value(serial); 
    assert(value->equals(value2)); //AUG
    Value* value3 = new Value((unsigned char*)"fhdjksalfhdjskal", 16);
    unsigned char* serial2 = value3->serialize();
    Value* value4 = new Value(serial2);
    assert(value3->equals(value4));
    assert(!value->equals(value3));
}

void put_test() {
    Key* key = new Key("839210-d21", 2387);
    Value* value = new Value((unsigned char*)"4jdky032fjcl*!(X", 16);
    Put* put1 = new Put(2312312, 98094, 8694053, key, value);
    unsigned char* serial = put1->serialize();
    Put* put2 = new Put(serial);
    assert(put1->equals(put2));
}

void get_test() {
    Key* key = new Key("rh3i412r3-13d43424930d32sxd", 321133);
    Get* get1 = new Get(23123, 6324, 26745, key);
    unsigned char* serial = get1->serialize();
    Get* get2 = new Get(serial);
    assert(get1->equals(get2));

    Key* key2 = new Key("rh3i413-m4qcn7&^SAD%c3h", 7893);
    Get* get3 = new Get(8904, 321, 58903, key2);
    unsigned char* serial2 = get3->serialize();
    Get* get4 = new Get(serial2);
    assert(get3->equals(get4));
    assert(!get1->equals(get3));
}

void result_test() {
    Value* value = new Value((unsigned char*)"4jdky032fjcl*!(X", 16);
    Result* res1 = new Result(2312312, 98094, 8694053, value);
    unsigned char* serial = res1->serialize();
    Result* res2 = new Result(serial);
    assert(res1->equals(res2));

    Value* value2 = new Value((unsigned char*)"zxvchjkl2543asdf809-", 20);
    Result* res3 = new Result(389025, 5789, 784231, value2);
    unsigned char* serial2 = res3->serialize();
    Result* res4 = new Result(serial2);
    assert(res3->equals(res4));

    assert(!res1->equals(res3));
}

int main() {
    size_t_test();
    success("Serial size_t");
    double_test();
    success("Serial double");
    string_test();
    success("Serial string");
    double_array_test();
    success("Serial double array");
    string_array_test();
    success("Serial string array");
    directory_test();
    success("Serial directory");
    ack_test();
    success("Serial ack");
    nack_test();
    success("Serial nack");
    status_test();
    success("Serial status");
    register_test();
    success("Serial register");
    put_test();
    success("Serial put");
    get_test();
    success("Serial get");
    result_test();
    success("Serial result");
    kill_test();
    success("Serial kill");
    network_utility();
    success("Serial network");
    schema_test();
    success("Serial schema");
    df_test();
    success("Serial DF");
    key_test();
    success("Serial key");
    value_test();
    success("Serial value");
}