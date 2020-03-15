#pragma once

#include "math.h"

#include "dataframe.h"
#include "fielder.h"

/**
 * Fielder to be used by the SumNumbers Rower
 */
class SumNumbersFielder : public Fielder {
    public:
        double sum_;

        SumNumbersFielder(double sum) {
            sum_ = sum;
        }

        void start(size_t r) {
            sum_ = 0;
        }

        void done() {}

        void accept(bool b) {}

        void accept(double d) {
            sum_ += d;
        }

        void accept(String* s) {}

        void accept(int i) {
            sum_ += i;
        }

        double get_sum() {
            return sum_;
        }

        ~SumNumbersFielder() {}
};

/**
 * Sums the numbers in the row, storing in the final column. 
 * Note: finalk column must be of type float
 * Authors:
 * Canon Sawrey sawrey.c@husky.neu.edu
 * Trevor Stenson stenson.t@husky.neu.edu */
class SumNumbers : public Rower {
    public:
        double sum_;
        SumNumbersFielder* sumFielder_;

        SumNumbers() {
            sum_ = 0;
            sumFielder_ = new SumNumbersFielder(sum_);
        }

        bool accept(Row& r) {
            r.visit(r.get_idx(), *sumFielder_);
            sum_ += sumFielder_->get_sum();
            return true;
        }

        Object* clone() {
            return new SumNumbers();
        }

        void join_delete(Rower* other) {
            SumNumbers *o = dynamic_cast<SumNumbers*>(other);
            assert(o != nullptr);
            sum_ += o->get_sum();
            delete o;
        }

        double get_sum() {
            return sum_;
        }

        ~SumNumbers() {
            delete sumFielder_;
        }
};

/**
 * Fielder to be used by the SumNumbers Rower
 */
class FibSumFielder : public Fielder {
    public:
        double total_fib_;
        int mod_ = 22;

        FibSumFielder(double sum) {
            total_fib_ = sum;
        }

        void start(size_t r) {
            total_fib_ = 0;
        }

        void done() {}

        void accept(bool b) {}

        void accept(double d) {
            total_fib_ += fib((int)d % mod_);
        }

        void accept(String* s) {}

        void accept(int i) {
            total_fib_ += fib(i % mod_);
        }

        double get_total_fib() {
            return total_fib_;
        }

        double fib(int i) {
            if (i == 0) {
                return 0;
            } else if (i == 1) {
                return 1;
            } else {
                return fib(i - 1) + fib(i - 2);
            }
        }

        ~FibSumFielder() {}
};

/**
 * Sums the numbers in the row, storing in the final column. 
 * Note: finalk column must be of type float
 * Authors:
 * Canon Sawrey sawrey.c@husky.neu.edu
 * Trevor Stenson stenson.t@husky.neu.edu */
class FibSum : public Rower {
    public:
        double sum_;
        FibSumFielder* fibSumFielder_;

        FibSum() {
            sum_ = 0;
            fibSumFielder_ = new FibSumFielder(sum_);
        }

        bool accept(Row& r) {
            r.visit(r.get_idx(), *fibSumFielder_);
            sum_ += fibSumFielder_->get_total_fib();
            return true;
        }

        Object* clone() {
            return new FibSum();
        }

        void join_delete(Rower* other) {
            FibSum *o = dynamic_cast<FibSum*>(other);
            assert(o != nullptr);
            sum_ += o->get_total_fib();
            delete o;
        }

        double get_total_fib() {
            return sum_;
        }

        ~FibSum() {
            delete fibSumFielder_;
        }
};


class HashingFielder : public Fielder {
    public:
        char* hash_;

        HashingFielder(char* start) {
            hash_ = start;
        }

        void start(size_t r) { }

        void done() {}

        void accept(bool b) {
            for (int i = 0; i < strlen(hash_); i++) {
                if (b) {
                    hash_[i] = hash_[i];
                } else {
                    hash_[i] = hash_[i];
                }
            }
        }

        void accept(double d) {
            for (int j = 0; j < floor(d); j++) {
                for (int i = 0; i < strlen(hash_); i++) {
                    hash_[i] = ((int)(hash_[i] + floor(d)) % 26) + 97;
                }
            }
        }

        void accept(String* s) {
            for (int i = 0; i < s->size(); i++) {
                for (int j = 0; j < strlen(hash_); j++) {
                    hash_[j] = ((hash_[j] + s->at(i)) % 26) + 97;
                }
            }
        }

        void accept(int i) {
            for (int k = 0; k < i % 5; k++) {
                for (int j = 0; j < strlen(hash_); j++) {
                    hash_[j] = ((hash_[j] + (i % 26)) % 26) + 97;
                }
            }
        }

        char* get_hash() {
            return hash_;
        }

        ~HashingFielder() {}
};

class HashingRower : public Rower {
    public:
        HashingFielder* hf_;
        char* start_;
        char* hash_;

        HashingRower(char* start) {
            hf_ = new HashingFielder(start);
            start_ = start;
            hash_ = start;
        }

        bool accept(Row& r) {
            r.visit(r.get_idx(), *hf_);
            hash_ = hf_->get_hash();
            return true;
        }

        Object* clone() {
            return new HashingRower(start_);
        }

        void join_delete(Rower* other) {
            HashingRower *o = dynamic_cast<HashingRower*>(other);
            assert(o != nullptr);
            char* o_hash = o->get_hash();
            for (int i = 0; i < strlen(hash_); i++) {
                hash_[i] = ((hash_[i] + o_hash[i]) % 26) + 97;
            }
            delete o;
        }

        char* get_hash() {
            return hash_;
        }

        ~HashingRower() {
            delete hf_;
        }
};

class InversionFielder : public Fielder {
    public:
        DataFrame* df_;
        DataFrame* new_df_;
        Row* newRow;
        size_t col_;
        size_t rowct;

        InversionFielder(DataFrame* df) {
            df_ = df;
            Schema* new_schema = new Schema(df->get_schema());
            new_df_ = new DataFrame(*new_schema);
            newRow = new Row(new_df_->get_schema());
            col_ = 0;
            rowct = 0;
        }

        void start(size_t r) {
            col_ = 0;
        }

        void done() {
            new_df_->add_row(*newRow);
            std::cout << "row added: " << rowct << std::endl;
            rowct++;
        }

        void accept(bool b) {
            newRow->set(col_, !b);
            col_++;
        }

        void accept(double d) {
            newRow->set(col_, -1 * d);
            col_++;
        }

        void accept(String* s) {
            newRow->set(col_, s);
            col_++;
        }

        void accept(int i) {
            newRow->set(col_, -1 * i);
            col_++;
        }

        DataFrame* get_new_dataframe() {
            return new_df_;
        }
};

class InversionRower : public Rower {
    public:
        DataFrame* df_;
        InversionFielder* inversionFielder_;

        InversionRower(DataFrame* df) {
            df_ = df;
            inversionFielder_ = new InversionFielder(df_);
        }

        bool accept(Row& r) {
            r.visit(r.get_idx(), *inversionFielder_);
            return true;
        }

        Object* clone() {
            return new InversionRower(df_);
        }

        void join_delete(Rower* other) {
            InversionRower *o = dynamic_cast<InversionRower*>(other);
            assert(o != nullptr);
            DataFrame* other_df = o->get_new_dataframe();
            DataFrame* new_df_ = inversionFielder_->get_new_dataframe();
            Row* tempRow = new Row(new_df_->get_schema());
            for (int i = 0; i < other_df->nrows(); i++) {
                other_df->fill_row(i, *tempRow);
                new_df_->add_row(*tempRow);
            }
            delete tempRow;
            delete o;
        }

        DataFrame* get_new_dataframe() {
            return inversionFielder_->get_new_dataframe();
        }

        ~InversionRower() {
            delete inversionFielder_;
        }
};

class ThreeLetterWordOccurrences : public Rower {
    public:
        DataFrame* df_;
        int occurrences;
        int count_;
        char** words;

        ThreeLetterWordOccurrences(DataFrame* df, int count, char** wrds) {
            for (int i = 0; i < count; i++) {
                assert(strlen(wrds[i]) == 3);
            }
            df_ = df;
            occurrences = 0;
            words = wrds;
            count_ = count;
        }

        bool accept(Row& r) {
            assert(df_->matchingSchema(r));
            for (size_t i = 0; i < r.width(); i++) {
                char type = r.col_type(i);
                switch(type) {
                    case 'S': {
                        char* str = r.get_string(i)->c_str();
                            for (int x = 0; x < count_; x++) {
                                if (strlen(str) >= strlen(words[x])) {
                                for (int i = 0; i < strlen(words[x]); i++) {
                                    if (str[i] == words[x][0]) {
                                        for (int j = i + 1; j < strlen(words[x]); j++) {
                                            if (str[j] == words[x][1]) {
                                                for (int k = j + 1; k < strlen(words[x]); k++) {
                                                    if (str[k] == words[x][2]) occurrences++;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
            return occurrences > 0;
        }

        Object* clone() {
            //cout << "ct: " << count_ << endl;
            char** newWords = new char*[count_];
            for (int i = 0; i < count_; i++) {
                //cout << "word: " << words[i] << endl << flush;
                newWords[i] = new char[4];
                strcpy(newWords[i], words[i]);
            }
            return new ThreeLetterWordOccurrences(df_, count_, newWords);
        }

        void join_delete(Rower* other) {
            ThreeLetterWordOccurrences *o = dynamic_cast<ThreeLetterWordOccurrences*>(other);
            assert(o != nullptr);
            occurrences += o->occurrences;
            delete o;
        }

        int get_occurrences() {
            return occurrences;
        }

        ~ThreeLetterWordOccurrences() {
            delete[] words;
        }
};

class GreaterThan30 : public Rower {
    public:
        DataFrame* df_;
        GreaterThan30(DataFrame* df) {
            df_ = df;
        }

        bool accept(Row& r) {
            assert(df_->matchingSchema(r));
            bool containsGreaterThan30 = false;
            for (size_t i = 0; i < r.width(); i++) {
                char type = r.col_type(i);
                switch(type) {
                    case 'I': {
                        int val = r.get_int(i);
                        if (val > 30) {
                            containsGreaterThan30 = true;
                        }
                        break;
                    }
                    case 'B': {
                        //do nothing
                        break;
                    }
                    case 'D': {
                        double val = r.get_double(i);
                        if (val > 30) {
                            containsGreaterThan30 = true;
                        }
                        break;
                    }
                    case 'S': {
                        //do nothing
                        break;
                    }
                }
            }
            return containsGreaterThan30;
        }

        void join_delete(Rower* other) {
            delete other;
        }
};