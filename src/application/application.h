#pragma once

#include <cstdlib>
#include "../object.h"
#include "../store/kvstore.h"

class Application : public Object {
    public:
        size_t idx_;
        KVStore kv;

        Application(size_t idx) {
            idx_ = idx;
            kv();
        }

        ~Application() { }

        virtual void run_();

        size_t this_node() {
            return idx_;
        }
};