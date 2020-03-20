#include "kvstore.h"
#include "../dataframe.h"

KVStore::KVStore() {
    kv_map_ = new map<Key*, Value*>;
}

KVStore::~KVStore() {
    delete kv_map_;
}

bool KVStore::containsKey(Key* k) {
    for (map<Key*, Value*>::iterator itr = kv_map_->begin(); itr != kv_map_->end(); ++itr) {
        Key* key = itr->first;
        if (key->equals(k)) return true;
    }
    return false;
}

Value* KVStore::put(Key* k, Value* v) {
    if (!containsKey(k)) {
        kv_map_->insert(pair<Key*, Value*>(k, v));
        return v;
    }
    return nullptr;
}

Value* KVStore::put(Key* k, unsigned char* data) {
    put(k, new Value(data));
}

DataFrame* KVStore::get(Key& k) {
    for (map<Key*, Value*>::iterator itr = kv_map_->begin(); itr != kv_map_->end(); ++itr) {
        Key* key = itr->first;
        printf("key: %s", key->name_->c_str());
        if (key->equals(&k)) {
            DataFrame* newdf = new DataFrame(itr->second->blob_);
            //for debugging
            newdf->print();
            return newdf;
        }
    }
    return nullptr;
}

Value* KVStore::getAndWait(Key& k) {
    return nullptr;
}