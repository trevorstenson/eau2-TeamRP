#pragma once
#include "../object.h"

class Visitor : public Object {
    public:
        Visitor() {}

        virtual void visit(Row& r) { }

        virtual bool done() {
            return true;
        }
};