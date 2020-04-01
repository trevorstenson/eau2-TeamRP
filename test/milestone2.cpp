#include "../src/application/trivial.h"
#include "assert.h"

void trivial_test() {
    Trivial* t = new Trivial(0);
    t->run_();
    delete t;
}

void map_test() {
    KVMap* kv = new KVMap();
    unsigned char xStr[] = "hey";
    unsigned char* data = &xStr[0];
    Key* key1 = new Key("first", 0);
    Value* v1 = new Value(data);
    kv->put(key1, v1);
    Value* v2 = kv->get(key1);
    assert(v2->equals(v1));
    assert(kv->containsKey(key1));
    printf("val1: %s\nval2: %s\n", v1->blob_, v2->blob_);
    kv->remove(key1);
    assert(!kv->containsKey(key1));
    Value* v3 = kv->get(key1);
    assert(v3 == nullptr);
    delete kv;
}

int main() {
    //map_test();
    trivial_test();
    return 0;
}