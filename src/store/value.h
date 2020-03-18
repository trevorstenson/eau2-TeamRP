#pragma once

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