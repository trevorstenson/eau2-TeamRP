//lang: cwc

#pragma once

#include "../object.h"

class Value : public Object {
    public:
        unsigned char* blob_;

        Value(unsigned char* blob) {
            blob_ = blob;
        }

        ~Value() {
            delete blob_;
        }
};