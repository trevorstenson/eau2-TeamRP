#include "application.h"
#include "../dataframe.h"
#include "../store/kvstore.h"

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
            DataFrame* testdf = new DataFrame(df->serialize());
            kv.put(key, df->serialize());
            assert(testdf->equals(df));
            assert(testdf->schema->equals(df->schema));
            DataFrame* df2 = kv.get(key);
            assert(df2->columns[0]->equals(df->columns[0]));
            Schema* s1 = df->schema;
            Schema* s2 = df2->schema;
            printf("schema 1:\nncol: %zu, nrow: %zu, col_cap: %zu\ntypes: %s\n", s1->n_col, s1->n_row, s1->col_cap, s1->types);
            printf("schema 2:\nncol: %zu, nrow: %zu, col_cap: %zu\ntypes: %s\n", s2->n_col, s2->n_row, s2->col_cap, s2->types);
            //df2->print();
            //for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
            //assert(sum==0);
            delete df; //delete df2;
        }
};
