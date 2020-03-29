#pragma once

#include "assert.h"
#include "stdarg.h"
#include "math.h"
#include <iostream>
#include "../string.h"
#include "../helper.h"

class IntColumn;
class BoolColumn;
class DoubleColumn;
class StringColumn;

using namespace std;
/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. 
 * Authors:
 * Canon Sawrey sawrey.c@husky.neu.edu
 * Trevor Stenson stenson.t@husky.neu.edu
 * */
class Column : public Object {
 public:
  char type;
  size_t len_;

  Column() { }
 
  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual IntColumn* as_int() {
    return nullptr;
  }
  virtual BoolColumn*  as_bool() {
    return nullptr;
  }
  virtual DoubleColumn* as_double() {
    return nullptr;
  }
  virtual StringColumn* as_string() {
    return nullptr;
  }
 
  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  virtual void push_back(int val) {
    assert("Invalid operation." && false);
  }
  virtual void push_back(bool val) {
    assert("Invalid operation." && false);
  }
  virtual void push_back(double val) {
    assert("Invalid operation." && false);
  }
  virtual void push_back(String* val) {
    assert("Invalid operation." && false);
  }

  size_t getParentIndex(size_t idx) {
    return (size_t)floor(log2(idx + 1));
  }

  size_t getChildIndex(size_t idx) {
    return idx - (pow(2, getParentIndex(idx)) - 1);
  }
 
 /** Returns the number of elements in the column. */
  virtual size_t size() {}
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'D'.*/
  char get_type() {
    assert(type != NULL);
    return type;
  }

  bool equals(Object * other) { 
    assert("Should not be calling equals on abstract class" && false);
  }

  /**Prints enmtire column */
  virtual void print() {}

  /** Prints value at i to std::cout */
  virtual void print(size_t i) {}
};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
  public:
    int** vals_;

  IntColumn() {
    type = 'I';
    len_ = 0;
    vals_ = new int*[20];
    for (int i = 0; i < 20; i++) {
      vals_[i] = nullptr;
    }
  }

  IntColumn(int n, ...) {
    type = 'I';
    va_list arguments;
    len_ = n;
    vals_ = new int*[20];
    for (int i = 0; i < 20; i++) {
      vals_[i] = nullptr;
    }

    va_start(arguments, n);
    for (int i = 0; i < n; i++) {
      ensureSubArray(i);
      int val = va_arg(arguments, int);
      size_t parentIndex = getParentIndex(i);
      int subIndex = getChildIndex(i);
      vals_[parentIndex][subIndex] = val;
    }
    va_end(arguments);
  }

  /**Ensures the smaller subarray is large enough/instantiated. */
  void ensureSubArray(size_t idx) {
    size_t arrayIndex = getParentIndex(idx);
    if (vals_[arrayIndex] == nullptr) {
      int count = (int)pow(2, arrayIndex);
      vals_[arrayIndex] = new int[count];
    }
  }

  /** Gets value at idx*/
  int get(size_t idx) {
    ensureSubArray(idx);
    return vals_[getParentIndex(idx)][getChildIndex(idx)];
  }

  IntColumn* as_int() {
    return this;
  }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, int val) {
    ensureSubArray(idx);
    if (idx >= len_) {
      for (size_t i = len_; i < idx; i++) {
        ensureSubArray(i);
        vals_[getParentIndex(i)][getChildIndex(i)] = NULL;
      }
      len_ = idx + 1;
    }
    vals_[getParentIndex(idx)][getChildIndex(idx)] = val;
  }

  void push_back(double val) {
    ensureSubArray(len_);
    vals_[getParentIndex(len_)][getChildIndex(len_)] = val;
    len_++;
  }

  size_t size() {
    return len_;
  }

  IntColumn* clone() {
    IntColumn* newColumn = new IntColumn();
    for (size_t i = 0; i < len_; i++) {
      newColumn->push_back(vals_[getParentIndex(i)][getChildIndex(i)]);
    }
    return newColumn;
  }

  void print(size_t i) {
    if (i < len_) {
      int outVal = vals_[getParentIndex(i)][getChildIndex(i)];
      if (outVal != NULL) {
        std::cout << outVal;
      }
    }
  }


  void print() {
    for (size_t i = 0; i < len_; i++) {
      int outVal = vals_[getParentIndex(i)][getChildIndex(i)];
      if (outVal != NULL) {
        std::cout << outVal << std::endl;
      } else {
        std::cout << "[NULL]" << std::endl;
      }
    }
  }

  bool equals(Object  * other) {
    if (this == other) return true;
    IntColumn* otherCol = dynamic_cast<IntColumn*>(other);
    if (otherCol == nullptr) return false;
    if (size() != otherCol->size()) return false;
    for (size_t i = 0; i < size(); i++) {
      if (get(i) != otherCol->get(i)) return false;
    }
    return true;
  }

  ~IntColumn() {
    for (size_t i = 0; i < len_; i++) {
      delete[] vals_[i];
    }
    delete[] vals_;
  }
};

/*************************************************************************
 * BoolColumn::
 * Holds int values.
 */
class BoolColumn : public Column {
  public:
    bool** vals_;

  BoolColumn() {
    type = 'B';
    len_ = 0;
    vals_ = new bool*[20];
    for (int i = 0; i < 20; i++) {
      vals_[i] = nullptr;
    }
  }

  BoolColumn(int n, ...) {
    type = 'B';
    va_list arguments;
    len_ = n;
    vals_ = new bool*[20];
    for (int i = 0; i < 20; i++) {
      vals_[i] = nullptr;
    }

    va_start(arguments, n);
    for (int i = 0; i < n; i++) {
      ensureSubArray(i);
      bool val = va_arg(arguments, int);
      size_t parentIndex = getParentIndex(i);
      int subIndex = getChildIndex(i);
      vals_[parentIndex][subIndex] = val;
    }
    va_end(arguments);
  }

  /**Ensures the smaller subarray is large enough/instantiated. */
  void ensureSubArray(size_t idx) {
    size_t arrayIndex = getParentIndex(idx);
    if (vals_[arrayIndex] == nullptr) {
      int count = (int)pow(2, arrayIndex);
      vals_[arrayIndex] = new bool[count];
    }
  }

  /** Gets value at idx*/
  bool get(size_t idx) {
    ensureSubArray(idx);
    return vals_[getParentIndex(idx)][getChildIndex(idx)];
  }

  BoolColumn* as_bool() {
    return this;
  }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, bool val) {
    ensureSubArray(idx);
    if (idx >= len_) {
      for (size_t i = len_; i < idx; i++) {
        ensureSubArray(i);
        vals_[getParentIndex(i)][getChildIndex(i)] = NULL;
      }
      len_ = idx + 1;
    }
    vals_[getParentIndex(idx)][getChildIndex(idx)] = val;
  }

  void push_back(bool val) {
    ensureSubArray(len_);
    vals_[getParentIndex(len_)][getChildIndex(len_)] = val;
    len_++;
  }

  size_t size() {
    return len_;
  }

  BoolColumn* clone() {
    BoolColumn* newColumn = new BoolColumn();
    for (size_t i = 0; i < len_; i++) {
      newColumn->push_back(vals_[getParentIndex(i)][getChildIndex(i)]);
    }
    return newColumn;
  }

  void print(size_t i) {
    if (i < len_) {
      bool outVal = vals_[getParentIndex(i)][getChildIndex(i)];
      if (outVal != NULL) {
        std::cout << outVal;
      } else {
        std::cout << "0";
      }
    }
  }

  void print() {
    for (size_t i = 0; i < len_; i++) {
      bool outVal = vals_[getParentIndex(i)][getChildIndex(i)];
      if (outVal != NULL) {
        if (outVal) {
          std::cout << outVal << std::endl;
        }
      } else {
        std::cout << "0" << std::endl;
      }
    }
  }

  bool equals(Object  * other) {
    if (this == other) return true;
    BoolColumn* otherCol = dynamic_cast<BoolColumn*>(other);
    if (otherCol == nullptr) return false;
    if (size() != otherCol->size()) return false;
    for (size_t i = 0; i < size(); i++) {
      if (get(i) != otherCol->get(i)) return false;
    }
    return true;
  }

  ~BoolColumn() {
    for (size_t i = 0; i < len_; i++) {
      delete[] vals_[i];
    }
    delete[] vals_;
  }
};

/*************************************************************************
 * DoubleColumn::
 * Holds double values.
 */
class DoubleColumn : public Column {
 public:
    double** vals_;

  DoubleColumn() {
    type = 'D';
    len_ = 0;
    vals_ = new double*[20];
    for (int i = 0; i < 20; i++) {
      vals_[i] = nullptr;
    }
  }

  DoubleColumn(int n, ...) {
    type = 'D';
    va_list arguments;
    len_ = n;
    vals_ = new double*[20];
    for (int i = 0; i < 20; i++) {
      vals_[i] = nullptr;
    }

    va_start(arguments, n);
    for (int i = 0; i < n; i++) {
      ensureSubArray(i);
      double val = va_arg(arguments, double);
      size_t parentIndex = getParentIndex(i);
      int subIndex = getChildIndex(i);
      vals_[parentIndex][subIndex] = val;
    }
    va_end(arguments);
  }

  /**Ensures the smaller subarray is large enough/instantiated. */
  void ensureSubArray(size_t idx) {
    size_t arrayIndex = getParentIndex(idx);
    if (vals_[arrayIndex] == nullptr) {
      int count = (int)pow(2, arrayIndex);
      vals_[arrayIndex] = new double[count];
    }
  }

  /** Gets value at idx*/
  double get(size_t idx) {
    ensureSubArray(idx);
    return vals_[getParentIndex(idx)][getChildIndex(idx)];
  }

  DoubleColumn* as_double() {
    return this;
  }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, double val) {
    ensureSubArray(idx);
    if (idx >= len_) {
      for (size_t i = len_; i < idx; i++) {
        ensureSubArray(i);
        vals_[getParentIndex(i)][getChildIndex(i)] = NULL;
      }
      len_ = idx + 1;
    }
    vals_[getParentIndex(idx)][getChildIndex(idx)] = val;
  }

  void push_back(double val) {
    ensureSubArray(len_);
    vals_[getParentIndex(len_)][getChildIndex(len_)] = val;
    len_++;
  }

  size_t size() {
    return len_;
  }

  DoubleColumn* clone() {
    DoubleColumn* newColumn = new DoubleColumn();
    for (size_t i = 0; i < len_; i++) {
      newColumn->push_back(vals_[getParentIndex(i)][getChildIndex(i)]);
    }
    return newColumn;
  }

  void print(size_t i) {
    if (i < len_) {
      double outVal = vals_[getParentIndex(i)][getChildIndex(i)];
      if (outVal != NULL) {
        std::cout << outVal;
      }
    }
  }

  void print() {
    for (size_t i = 0; i < len_; i++) {
      double outVal = vals_[getParentIndex(i)][getChildIndex(i)];
      if (outVal != NULL) {
        std::cout << outVal << std::endl;
      } else {
        std::cout << "[NULL]" << std::endl;
      }
    }
  }

  bool equals(Object  * other) {
    if (this == other) return true;
    DoubleColumn* otherCol = dynamic_cast<DoubleColumn*>(other);
    if (otherCol == nullptr) return false;
    if (size() != otherCol->size()) return false;
    for (size_t i = 0; i < size(); i++) {
      if (get(i) != otherCol->get(i)) return false;
    }
    return true;
  }

  ~DoubleColumn() {
    for (size_t i = 0; i < len_; i++) {
      delete[] vals_[i];
    }
    delete[] vals_;
  }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column {
  public:
    String*** vals_;

  StringColumn() {
    type = 'S';
    len_ = 0;
    vals_ = new String**[20];
    for (int i = 0; i < 20; i++) {
      vals_[i] = nullptr;
    }
  }

  StringColumn(int n, ...) {
    type = 'S';
    va_list arguments;
    len_ = n;
    vals_ = new String**[20];
    for (int i = 0; i < 20; i++) {
      vals_[i] = nullptr;
    }

    va_start(arguments, n);
    for (int i = 0; i < n; i++) {
      ensureSubArray(i);
      String* val = va_arg(arguments, String*);
      size_t parentIndex = getParentIndex(i);
      int subIndex = getChildIndex(i);
      vals_[parentIndex][subIndex] = val;
    }
    va_end(arguments);
  }

  /**Ensures the smaller subarray is large enough/instantiated. */
  void ensureSubArray(size_t idx) {
    size_t arrayIndex = getParentIndex(idx);
    if (vals_[arrayIndex] == nullptr) {
      int count = (int)pow(2, arrayIndex);
      vals_[arrayIndex] = new String*[count];
    }
  }

  /** Gets value at idx*/
  String* get(size_t idx) {
    ensureSubArray(idx);
    return vals_[getParentIndex(idx)][getChildIndex(idx)];
  }

  StringColumn* as_string() {
    return this;
  }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, String* val) {
    ensureSubArray(idx);
    if (idx >= len_) {
      for (size_t i = len_; i < idx; i++) {
        ensureSubArray(i);
        vals_[getParentIndex(i)][getChildIndex(i)] = nullptr;
      }
      len_ = idx + 1;
    }
    vals_[getParentIndex(idx)][getChildIndex(idx)] = val;
  }

  void push_back(String* val) {
    ensureSubArray(len_);
    vals_[getParentIndex(len_)][getChildIndex(len_)] = val;
    len_++;
  }

  size_t size() {
    return len_;
  }

  StringColumn* clone() {
    StringColumn* newColumn = new StringColumn();
    for (size_t i = 0; i < len_; i++) {
      newColumn->push_back(vals_[getParentIndex(i)][getChildIndex(i)]->clone());
    }
    return newColumn;
  }

  void print(size_t i) {
    if (i < len_) {
      String* outVal = vals_[getParentIndex(i)][getChildIndex(i)];
      if (outVal != nullptr) {
        std::cout << outVal->c_str();
      }
    }
  }

  void print() {
    for (size_t i = 0; i < len_; i++) {
      String* outVal = vals_[getParentIndex(i)][getChildIndex(i)];
      if (outVal != nullptr) {
        std::cout << outVal->c_str() << std::endl;
      } else {
        std::cout << "[nullptr]" << std::endl;
      }
    }
  }

  bool equals(Object  * other) {
    if (this == other) return true;
    StringColumn* otherCol = dynamic_cast<StringColumn*>(other);
    if (otherCol == nullptr) return false;
    if (size() != otherCol->size()) return false;
    for (size_t i = 0; i < size(); i++) {
      if (!get(i)->equals(otherCol->get(i))) return false;
    }
    return true;
  }

  ~StringColumn() {
    for (size_t i = 0; i < len_; i++) {
      delete[] vals_[i];
    }
    delete[] vals_;
  }
};