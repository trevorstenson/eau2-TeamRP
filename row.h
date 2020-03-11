//lang: CwC
#pragma once

#include "schema.h"
#include "fielder.h"

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality, note: it was not overriden from object on purpose
 * Authors:
 * Canon Sawrey sawrey.c@husky.neu.edu
 * Trevor Stenson stenson.t@husky.neu.edu
 */
class Row : public Object {
 public:
  Schema* schema_;
  Column** columns_;
  int idx_;
  int width_;

  /** Build a row following a schema. */
  Row(Schema& scm) {
    schema_ = &scm;
    width_ = schema_->width();
    columns_ = new Column*[width_];
    for (int i = 0; i < width_; i++) {
      columns_[i] = nullptr;
    }
  }
 
  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, int val) {
    if (schema_->col_type(col) == 'I') {
      if (columns_[col] != nullptr) {
        delete columns_[col];
      }
      columns_[col] = new IntColumn();
      columns_[col]->as_int()->set(0, val);
    } else {
      assert("Wrong column type for given value." && false);
    }
  }

  void set(size_t col, float val) {
    if (schema_->col_type(col) == 'F') {
      if (columns_[col] != nullptr) {
        delete columns_[col];
      }
      columns_[col] = new FloatColumn();
      columns_[col]->as_float()->set(0, val);
    } else {
      assert("Wrong column type for given value." && false);
    }
  }

  void set(size_t col, bool val) {
    if (schema_->col_type(col) == 'B') {
      if (columns_[col] != nullptr) {
        delete columns_[col];
      }
      columns_[col] = new BoolColumn();
      columns_[col]->as_bool()->set(0, val);
    } else {
      assert("Wrong column type for given value." && false);
    }
  }

  /** The string is external. */
  void set(size_t col, String* val) {
    if (schema_->col_type(col) == 'S') {
      if (columns_[col] != nullptr) {
        delete columns_[col];
      }
      columns_[col] = new StringColumn();
      columns_[col]->as_string()->set(0, (val == nullptr) ? nullptr : val->clone());
    } else {
      assert("Wrong column type for given value." && false);
    }
  }
 
  /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
  void set_idx(size_t idx) {
    //std::cout << "idx: " << idx << std::endl;
    idx_ = idx;
  }
  size_t get_idx() {
    return idx_;
  }
 
  /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. */
  int get_int(size_t col) {
    if (schema_->col_type(col) == 'I') {
      if (columns_[col]->get_type() == 'I') {
        //IntColumn* intCol = dynamic_cast<IntColumn*>(columns_[col]);
        return columns_[col]->as_int()->get(0);
      }
    }
    assert("Wrong type. Program terminated." && false);
  }

  bool get_bool(size_t col) {
    if (schema_->col_type(col) == 'B') {
      if (columns_[col]->get_type() == 'B') {
        //BoolColumn* boolCol = dynamic_cast<BoolColumn*>(columns_[col]);
        return columns_[col]->as_bool()->get(0);
      }
    }
    assert("Wrong type. Program terminated." && false);
  }

  float get_float(size_t col) {
    if (schema_->col_type(col) == 'F') {
      if (columns_[col]->get_type() == 'F') {
        //FloatColumn* floatCol = dynamic_cast<FloatColumn*>(columns_[col]);
        return columns_[col]->as_float()->get(0);
      }
    }
    assert("Wrong type. Program terminated." && false);
  }

  String* get_string(size_t col) {
    if (schema_->col_type(col) == 'S') {
      if (columns_[col]->get_type() == 'S') {
        //StringColumn* strCol = dynamic_cast<StringColumn*>(columns_[col]);
        return columns_[col]->as_string()->get(0);
      }
    }
    assert("Wrong type. Program terminated." && false);
  }

  Schema* get_schema() {
    return schema_;
  }
 
  /** Number of fields in the row. */
  size_t width() {
    return width_;
  }
 
   /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) {
    return columns_[idx]->get_type();
  }
 
  /** Given a Fielder, visit every field of this row. The first argument is
    * index of the row in the dataframe.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
    if (idx == idx_) {
      f.start(idx);
      for (size_t i = 0; i < width_; i++) {
        char type = col_type(i);
        switch (type) {
          case 'I': {
            IntColumn* intCol = columns_[i]->as_int();
            f.accept(intCol->get(0));
            break;
          }
          case 'B': {
            BoolColumn* bCol = columns_[i]->as_bool();
            f.accept(bCol->get(0));
            break;
          }
          case 'F': {
            FloatColumn* fCol = columns_[i]->as_float();
            f.accept(fCol->get(0));
            break;
          }
          case 'S': {
            StringColumn* sCol = columns_[i]->as_string();
            f.accept(sCol->get(0));
            break;
          }
          default:
            assert("Invalid type. Program terminated." && false);
        }
      }
      f.done();
    }
  }

  /** Subclass redefinition. Pointer equality */
  virtual bool equals(Object  * other) { 
    return this == other;  
  }

  void print() {
    std::cout << "------" << std::endl;
    for (size_t i = 0; i < width_; i++) {
      columns_[i]->print();
    }
    std::cout << "------" << std::endl;
  }

  ~Row() {
    delete[] columns_;
  }
};