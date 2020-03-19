#include "../src/array.h"
#include "../src/message.h"
#include "../src/serial.h"
#include "../../dataframe.h"

using namespace std;

void size_t_test() {
    size_t a = 81;
    unsigned char* a_serial = serialize_size_t(a);
    size_t a_deserial = deserialize_size_t(a_serial);
    assert(a == a_deserial);

    size_t b = 578349013;
    unsigned char* b_serial = serialize_size_t(b);
    size_t b_deserial = deserialize_size_t(b_serial);
    assert(b == b_deserial);

    size_t c = 12301;
    unsigned char* c_serial = serialize_size_t(c);
    size_t c_deserial = deserialize_size_t(c_serial);
    assert(c == c_deserial);

    size_t d = 0;
    unsigned char* d_serial = serialize_size_t(d);
    size_t d_deserial = deserialize_size_t(d_serial);
    assert(d == d_deserial);

    cout << "*****   Passed: size_t utility   *****\n";
}

void double_test() {
    double a = 421.3213;
    unsigned char* a_serial = serialize_double(a);
    double a_deserial = deserialize_double(a_serial);
    assert(a == a_deserial);

    double b = 0.24124;
    unsigned char* b_serial = serialize_double(b);
    double b_deserial = deserialize_double(b_serial);
    assert(b == b_deserial);

    double c = 213343241.123;
    unsigned char* c_serial = serialize_double(c);
    double c_deserial = deserialize_double(c_serial);
    assert(c == c_deserial);

    double d = 1032136950.4326217;
    unsigned char* d_serial = serialize_double(d);
    double d_deserial = deserialize_double(d_serial);
    assert(d == d_deserial);

    cout << "*****   Passed: double utility   *****\n";
}

void string_test() {
    String* a = new String("Hello");
    unsigned char* a_serial = serialize_string(a);
    String* a_deserial = deserialize_string(a_serial);
    assert(a->equals(a_deserial));

    String* b = new String("&cwd189a&%^C&WQ");
    unsigned char* b_serial = serialize_string(b);
    String* b_deserial = deserialize_string(b_serial);
    assert(b->equals(b_deserial));

    String* c = new String("");
    unsigned char* c_serial = serialize_string(c);
    String* c_deserial = deserialize_string(c_serial);
    assert(c->equals(c_deserial));

    cout << "*****   Passed: String* utility   *****\n";
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
    cout << "*****   Passed: Double Array   *****\n";
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
    cout << "*****   Passed: String Array   *****\n";
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
    cout << "*****   Passed: Directory   *****\n";
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
    cout << "*****   Passed: Ack   *****\n";
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
    cout << "*****   Passed: Nack   *****\n";
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
    assert(!message->equals(message_deserialized));
    cout << "*****   Passed: Status   *****\n";
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
    cout << "*****   Passed: Register   *****\n";
}

void network_utility() {
    Ack* ack = new Ack();
    ack->kind_ = MsgKind::Ack;
    unsigned char* ack2 = ack->serialize();
    assert(MsgKind::Ack == message_kind(ack2));
    assert(34 == message_length(ack2));

    Nack* nack = new Nack();
    nack->kind_ = MsgKind::Nack;
    unsigned char* nack2 = nack->serialize();
    assert(MsgKind::Nack == message_kind(nack2));
    assert(34 == message_length(ack2));

    Status* status = new Status();
    status->kind_ = MsgKind::Status;
    status->msg_ = new String("hello");
    unsigned char* status2 = status->serialize();
    assert(MsgKind::Status == message_kind(status2));
    assert(39 == message_length(status2));
    cout << "*****   Passed: Networking Utilities   *****\n";
}

void kill_test() {
    Kill* kill = new Kill();
    unsigned char* kill2 = kill->serialize();
    assert(MsgKind::Kill == message_kind(kill2));
    assert(33 == message_length(kill2));
    Kill* kill_d = new Kill(kill2);
    assert(kill->equals(kill_d));

    cout << "*****   Passed: Kill   *****\n";
}

void schema_test() {
    Schema* schema = new Schema("BDBSISDBSDI");
    schema->new_length(18);
    unsigned char* serial = schema->serialize();
    Schema* schema2 = new Schema(serial);
    assert(schema->equals(schema2));

    cout << "*****   Passed: Schema   *****\n";
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
    assert(df2->equals(df));
    cout << "*****   Passed: DataFrame   *****\n";
}

int main() {
    cout << "Running serialization tests...\n";
    size_t_test();
    double_test();
    string_test();
    double_array_test();
    string_array_test();
    directory_test();
    ack_test();
    nack_test();
    status_test();
    register_test();
    kill_test();
    network_utility();
    schema_test();
    df_test();
    cout << "*****   All serialization tests passed   *****\n";
}