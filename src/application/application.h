#pragma once

#include <cstdlib>
#include "../object.h"
#include "../dataframe/dataframe.h"

class Application : public Object {
    public:
        size_t idx_;
        KVStore kv;

        Application(size_t idx) {
            idx_ = idx;
            kv.setIndex(idx);
        }

        virtual void run_() { }

        void setMockNetwork(KVStore** mockNetwork) {
            kv.setMockNetwork(mockNetwork);
        }

        size_t this_node() {
            return idx_;
        }
};