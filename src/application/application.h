#pragma once

#include <cstdlib>
#include "../object.h"
#include "../dataframe.h"

class Application : public Object {
    public:
        size_t idx_;
        KVStore kv;
        KVStore** mockNetwork_;


        Application(size_t idx) {
            idx_ = idx;
            kv.setApplication(this);
        }

        virtual void run_() {
            
        }

        void setMockNetwork(KVStore** mockNetwork) {
            mockNetwork_ = mockNetwork;
        }

        Value* requestValue(Key& k) {
            mockNetwork_[k.node_]->get(k);
        }

        size_t this_node() {
            return idx_;
        }
};