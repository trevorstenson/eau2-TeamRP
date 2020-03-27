#include "application.h"

class Trivial : public Application {
    public:
        Trivial(size_t idx) : Application(idx) { }

        void run_() {
            size_t SZ = 1000*1000;
            double* vals = new double[SZ];
            double sum = 0;
            for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
            Key key("triv",0);
            DataFrame* df = DataFrame::fromArray(&key, &kv, SZ, vals);
            assert(df->get_double(0,1) == 1);
            DataFrame* df2 = kv.get(key);
            for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
            assert(sum==0);
            Key ver("check", 0);
            DataFrame* onevaldf = DataFrame::fromScalar(&key, &kv, 345.6);
            DataFrame* twovaldf = kv.get(ver);
            onevaldf->print();
            twovaldf->print();
            delete df; delete df2;
        }
};
