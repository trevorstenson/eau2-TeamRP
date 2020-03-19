#pragma once

#include <map>
#include "key.h"
#include "value.h"

using namespace std;

class KVStore : public Object  {
    public:
        map<Key*, Value*>* kv_map_;

        KVStore() {
            kv_map_ = new map<Key*, Value*>;
        }

        ~KVStore() {
            //delete properly
            delete kv_map_;
        }

        bool containsKey(Key* k) {
            for (map<Key*, Value*>::iterator itr = kv_map_->begin(); itr != kv_map_->end(); ++itr) {
                Key* key = itr->first;
                if (key->equals(k)) return true;
            }
            return false;
        }

        Value* put(Key* k, Value* v) {
            if (!containsKey(k)) {
                kv_map_->insert(pair<Key*, Value*>(k, v));
                return v;
            }
            return nullptr;
        }

        Value* get(Key& k) {
            for (map<Key*, Value*>::iterator itr = kv_map_->begin(); itr != kv_map_->end(); ++itr) {
                Key* key = itr->first;
                if (key->equals(&k)) return itr->second;
            }
            return nullptr;
        }

        Value* getAndWait(Key& k) {
            return nullptr;
        }
};