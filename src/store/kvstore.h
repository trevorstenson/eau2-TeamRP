#pragma once

#include <map>
#include "../object.h"
#include "key.h"
#include "value.h"
#include "../dataframe.h"

class DataFrame;

using namespace std;

class KVStore : public Object  {
    public:
        map<Key*, Value*>* kv_map_;
        // KVStore();
        // ~KVStore();
        // bool containsKey(Key* k);
        // Value* put(Key* k, Value* v);
        // Value* put(Key& k, unsigned char* data);
        // DataFrame* get(Key& k);
        // Value* getAndWait(Key& k);

        KVStore() {
    kv_map_ = new map<Key*, Value*>;
}

~KVStore() {
    delete kv_map_;
}

bool containsKey(Key* k) {
    for (map<Key*, Value*>::iterator itr = kv_map_->begin(); itr != kv_map_->end(); ++itr) {
        Key* key = itr->first;
        if (key->equals(k)) return true;
    }
    return false;
}

Value* put(Key& k, Value* v) {
    if (!containsKey(&k)) {
        kv_map_->insert(pair<Key*, Value*>(&k, v));
        return v;
    }
    return nullptr;
}

Value* put(Key& k, unsigned char* data) {
    put(k, new Value(data));
}

DataFrame* get(Key& k) {
    for (map<Key*, Value*>::iterator itr = kv_map_->begin(); itr != kv_map_->end(); ++itr) {
        Key* key = itr->first;
        printf("key: %s", key->name_->c_str());
        if (key->equals(&k)) {
            DataFrame* newdf = new DataFrame(itr->second->blob_);
            //for debugging
            //newdf->print();
            return newdf;
        }
    }
    return nullptr;
}

Value* getAndWait(Key& k) {
    return nullptr;
}
};