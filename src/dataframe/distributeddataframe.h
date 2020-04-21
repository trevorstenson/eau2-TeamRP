#pragma once

#include "dataframe.h"
#include <algorithm>

#define ROWS_PER_DF 1

class DistributedDataFrame : public DataFrame {
    public:
        size_t nodes_;
        KVStore* kv_;
        String* uid_;
        vector<int> sub_ids;

        DistributedDataFrame(Schema &schema_) {
            this->schema = &schema_;
        }

        DistributedDataFrame(Schema &schema_, size_t nodeCount, KVStore &kv, String* uid) {
            this->schema = &schema_;
            this->nodes_ = nodeCount;
            this->kv_ = &kv;
            this->uid_ = uid->clone();
        }

        ~DistributedDataFrame() {
            delete uid_;
            delete schema;
        }

        void set(size_t col, size_t row, int val) {
            DataFrame* df = getDFwithRow(row);
            df->set(col, getInternalRow(row), val);
            setDFwithRow(row, df);
        }

        void set(size_t col, size_t row, double val) {
            DataFrame* df = getDFwithRow(row);
            df->set(col, getInternalRow(row), val);
            setDFwithRow(row, df);
        }

        void set(size_t col, size_t row, bool val) {
            DataFrame* df = getDFwithRow(row);
            df->set(col, getInternalRow(row), val);
            setDFwithRow(row, df);
        }

        void set(size_t col, size_t row, String* val) {
            DataFrame* df = getDFwithRow(row);
            df->set(col, getInternalRow(row), val);
            setDFwithRow(row, df);
        }

        int get_int(size_t col, size_t row) {
            DataFrame* df = getDFwithRow(row);
            return df->get_int(col, getInternalRow(row));
        }

        double get_double(size_t col, size_t row) {
            DataFrame* df = getDFwithRow(row);
            return df->get_double(col, getInternalRow(row));
        }

        bool get_bool(size_t col, size_t row) {
            DataFrame* df = getDFwithRow(row);
            return df->get_bool(col, getInternalRow(row));
        }

        String* get_string(size_t col, size_t row) {
            DataFrame* df = getDFwithRow(row);
            return df->get_string(col, getInternalRow(row));
        }

        DataFrame* getDFwithRow(size_t row) {
            printf("nice");
            if (std::find(sub_ids.begin(), sub_ids.end(), getDFid(row)) != sub_ids.end()) {
                printf("nice2");
                Key* k = createKeyFromRow(row);
                DataFrame* df = kv_->waitAndGet(*k);
                delete k;
                return df;
            }
            printf("nice3");
            return new DataFrame(*schema);
        }

        DataFrame* setDFwithRow(size_t row, DataFrame* df) {
            if (std::find(sub_ids.begin(), sub_ids.end(), getDFid(row)) == sub_ids.end())
                sub_ids.push_back(getDFid(row));
            Key* k = createKeyFromRow(row);
            unsigned char* serial = df->serialize();
            kv_->put(*k, serial, extract_size_t(serial, 0));
            delete k;
        }

        size_t getDFid(size_t row) {
            return row / ROWS_PER_DF;
        }

        Key* createKeyFromRow(size_t row) {
            StrBuff* sb = new StrBuff();
            sb->c(*uid_);
            sb->c(getDFid(row));
            Key* finalKey = new Key(sb->get(), getNodeFromRow(row));
            delete sb;
            return finalKey;
        }

        size_t getNodeFromRow(size_t row) {
            size_t baseRow = row / ROWS_PER_DF;
            return row % nodes_;
        }

        size_t getInternalRow(size_t row) {
            row % ROWS_PER_DF;
        }
};