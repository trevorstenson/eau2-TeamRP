//lang: CwC
#pragma once

#include "column.h"
#include "row.h"
#include "rower.h"
#include "schema.h"
#include "serial/src/serial.h"
#include "serial/src/array.h"
#include "object.h"
#include <thread>
#include <mutex>
#include <functional>
#include <string>

//KVStore
#include "map.h"
#include "store/key.h"
#include "store/value.h"
#include "application/application.h"

#define MAX_THREADS 8
#define MIN_LINES 1000

class DataFrame;
class Key;
class Value;
class Application;

class KVStore : public Object  {
    public:
        KVMap kv_map_;
        Application* app_;
        KVStore();
        ~KVStore();
        void setApplication(Application* app);
        bool containsKey(Key* k);
        Value* put(Key& k, Value* v);
        Value* put(Key& k, unsigned char* data);
        DataFrame* get(Key& k);
        Value* waitAndGet(Key& k);
};

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 * Authors:
 * Canon Sawrey sawrey.c@husky.neu.edu
 * Trevor Stenson stenson.t@husky.neu.edu
 */
class DataFrame : public Object, public Serializable {
 public:
  Schema* schema;
  size_t col_cap;
  Column** columns;
 
  /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
  DataFrame(DataFrame& df) {
    Schema* newSchema = new Schema(df.schema->types);
    schema = newSchema;
    col_cap = df.col_cap;
    columns = new Column*[col_cap];
    for (int i = 0; i < schema->width(); i++) {
      switch (schema->type(i)) {
        case 'B':
          columns[i] = new BoolColumn();
          break;
        case 'I':
          columns[i] = new IntColumn();
          break;
        case 'S':
          columns[i] = new StringColumn();
          break;
        case 'D':
          columns[i] = new DoubleColumn();
          break;
        default:
          assert("Type other than B, I, F, or S found." && false);
      }
    }
  }
 
  /** Create a data frame from a schema and columns. All columns are created
    * empty. */
  DataFrame(Schema& schema_) {
    schema = &schema_;
    col_cap = schema->col_cap;
    columns = new Column*[col_cap];
    for (int i = 0; i < schema->width(); i++) {
      switch (schema->type(i)) {
        case 'B':
          columns[i] = new BoolColumn();
          break;
        case 'I':
          columns[i] = new IntColumn();
          break;
        case 'S':
          columns[i] = new StringColumn();
          break;
        case 'D':
          columns[i] = new DoubleColumn();
          break;
        default:
          assert("Type other than B, I, F, or S found." && false);
      }
    }
  }

  DataFrame(unsigned char* serial) {
    deserialize(serial);
  }
 
  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    //Schema* temp = new Schema(schema); 
    //return *temp;
    return *schema;
  }
 
  /** Adds a column this dataframe, updates the schema, the new column
    * is external, and appears as the last column of the dataframe, the
    * name is optional and external. A nullptr colum is undefined. */
  void add_column(Column* col, String* name) {
    if (col == nullptr) {
      assert("Cannot have nullptr col." && false);
    }
    //Copy column
    Column* addCol;
    switch (col->type) {
      case 'B':
        addCol = col->as_bool()->clone();
        break;
      case 'D':
        addCol = col->as_double()->clone();
        break;
      case 'I':
        addCol = col->as_int()->clone();
        break;
      case 'S':
        addCol = col->as_string()->clone();
        break;    
      default:
        break;
    }
    ensureColumnCapacity();
    columns[schema->width()] = addCol;
    schema->new_length(addCol->size());
    schema->add_column(col->get_type(), name);
  }

  /** Ensures the current col capacity is enough to
   * accomodate an addition */
  void ensureColumnCapacity() {
    if (schema->width() == col_cap) {
      growColumns();
    }
  }

  /** Increase columm capacity and copies over old values */
  void growColumns() {
    size_t previous = col_cap;
    if (col_cap == 0) {
      col_cap++;
    } else {
      col_cap *= 2;
    }
    Column** newValues = new Column*[col_cap];
    for (size_t i = 0; i < previous; i++) {
      newValues[i] = columns[i];
    }
    delete[] columns;
    columns = newValues;
  }
 
  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row) {
    checkIndices(col, row, 'I');
    return columns[col]->as_int()->get(row);
  }
  bool get_bool(size_t col, size_t row) {
    checkIndices(col, row, 'B');
    return columns[col]->as_bool()->get(row);
  }
  double get_double(size_t col, size_t row) {
    checkIndices(col, row, 'D');
    return columns[col]->as_double()->get(row);
  }
  String* get_string(size_t col, size_t row) {
    checkIndices(col, row, 'S');
    return columns[col]->as_string()->get(row);
  }
 
  /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
  void set(size_t col, size_t row, int val) {
    checkIndices(col, row, 'I');
    schema->new_length(row);
    IntColumn* column = columns[col]->as_int();
    if (!column) {
      assert("Unable to cast as IntColumn." && false);
    } else {
      column->set(row, val);
    }
  }
  void set(size_t col, size_t row, bool val) {
    checkIndices(col, row, 'B');
    schema->new_length(row);
    BoolColumn* column = columns[col]->as_bool();
    if (!column) {
      assert("Unable to cast as BoolColumn." && false);
    } else {
      column->set(row, val);
    }
  }
  void set(size_t col, size_t row, double val){
    checkIndices(col, row, 'D');
    schema->new_length(row);
    DoubleColumn* column = columns[col]->as_double();
    if (!column) {
      assert("Unable to cast as DoubleColumn." && false);
    } else {
      column->set(row, val);
    }
  }
  void set(size_t col, size_t row, String* val) {
    checkIndices(col, row, 'S');
    schema->new_length(row);
    StringColumn* column = columns[col]->as_string();
    if (!column) {
      assert("Unable to cast as StringColumn." && false);
    } else {
      column->set(row, val);
    }
  }

  void checkIndices(size_t col, size_t row, char type) {
    if (col >= schema->n_col) {
      assert("Index out of bounds." && false);
    } else if (schema->type(col) != type) {
      assert("Type mismatch." && false);
    }
  }
 
  /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not form the same schema as the
    * dataframe, results are undefined.
    */
  void fill_row(size_t idx, Row& row) {
    if (matchingSchema(row)) {
      row.set_idx(idx);
      for (size_t i = 0; i < ncols(); i++) {
        char type = columns[i]->get_type();
        switch (type) {
          case 'I': {
            IntColumn* intCol = columns[i]->as_int();
            row.set(i, intCol->get(idx));
            break;
          }
          case 'B': {
            BoolColumn* boolCol = columns[i]->as_bool();
            row.set(i, boolCol->get(idx));
            break;
          }
          case 'D': {
            DoubleColumn* fCol = columns[i]->as_double();
            row.set(i, fCol->get(idx));
            break;
          }
          case 'S': {
            StringColumn* sCol = columns[i]->as_string();
            row.set(i, sCol->get(idx));
            break;
          }
          default:
            assert("Invalid operation." && false);
        }
      }
    }
  }
 
  /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undedined.  */
  void add_row(Row& row) {
    if (!matchingSchema(row)) {
      assert("Incorrect row schema." && false);
    } else {
      set_row(nrows(), row);
    }
  }

  void set_row(size_t idx, Row& row) {
    if (!matchingSchema(row)) {
      assert("Incorrect row schema." && false);
    } else {
      //Update schema to know the number of rows - use idx + 1 to get length
      schema->new_length(idx);
      //Load data into columns
      for (size_t i = 0; i < row.width(); i++) {
        char type = schema->type(i);
        switch (type) {
          case 'I': {
            columns[i]->as_int()->set(idx, row.get_int(i));  
            break;
          }
          case 'B': {
            columns[i]->as_bool()->set(idx, row.get_bool(i));  
            break;
          }
          case 'D': {
            columns[i]->as_double()->set(idx, row.get_double(i));  
            break;
          }
          case 'S': {
            columns[i]->as_string()->set(idx, row.get_string(i));  
            break;
          }
          default:
            assert("Invalid type. Program terminated." && false);
        }
      }
    }
  }

  /** Determines if the given row matches the schema of 
   * this Schema* */
  bool matchingSchema(Row& row) {
    return strcmp(row.get_schema()->types, schema->types) == 0;
  }
 
  /** The number of rows in the dataframe. */
  size_t nrows() {
    return schema->length();
  }
 
  /** The number of columns in the dataframe.*/
  size_t ncols() {
    return schema->width();
  }
 
  /** Visit rows in order */
  void map(Rower& r) {
    for (size_t i = 0; i < nrows(); i++) {
      Row newRow(get_schema());
      fill_row(i, newRow);
      r.accept(newRow);
    }
  }

    //this method kept throwing errors when attempting to use for the first and last halves.
  void pmapRange(size_t start, size_t end, Rower* r) {
      for (size_t i = start; i < end; i++) {
          Row newRow(get_schema());
          fill_row(i, newRow);
          r->accept(newRow);
      }
  }

  void pmap(Rower& r) {
      size_t threadCount = nrows() / MIN_LINES;
      if (threadCount <= 1) {
        map(r);
      } else {
        threadCount = (threadCount > MAX_THREADS) ? MAX_THREADS : threadCount;
        size_t rowsPerThread = nrows() / threadCount;

        Rower** rowers = new Rower*[threadCount];
        rowers[0] = &r;
        for (int i = 1; i < threadCount; i++) {
          rowers[i] = static_cast<Rower*>(r.clone());
        }

        std::thread* threads[threadCount];
        int startingValue = 0;
        int endValue = 0;
        for (int i = 0; i < threadCount; i++) {
          if (threadCount - i == 1) {
            endValue = nrows();
          } else {
            endValue = startingValue + rowsPerThread;
          }
          threads[i] = new std::thread(&DataFrame::pmapRange, this, startingValue, endValue, rowers[i]);
          startingValue = endValue;
        }
        
        for (int i = 0; i < threadCount; ++i) {
          threads[i]->join();
        }

        for (int i = 1; i < threadCount; i++) {
          rowers[0]->join_delete(rowers[i]);
        }

        for (int i = 0; i < threadCount; i++) {
          delete threads[i];
        }

        delete[] rowers;
      }
      
  }
 
  /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
  DataFrame* filter(Rower& r) {
    DataFrame* newDataFrame = new DataFrame(*this);
    int nr = nrows();
    for (size_t i = 0; i < nr; i++) {
      Row* newRow = new Row(*newDataFrame->schema);
      fill_row(i, *newRow);
      if (r.accept(*newRow)) {
        newDataFrame->add_row(*newRow);
      }
    }
    return newDataFrame;
  }

  static DataFrame* fromArray(Key* key, KVStore* kv, size_t size, double* array) {
    String* schemaStr = new String("D");
    Schema* newSchema = new Schema(schemaStr->c_str());
    delete schemaStr;
    DataFrame* newDf = new DataFrame(*newSchema);
    for (size_t i = 0; i < size; i++) {
      newDf->set(0, i, array[i]);
    }
    //serialize and add df to kvstore
    kv->put(*key, newDf->serialize());
    return newDf;
  }

  static DataFrame* fromScalar(Key* key, KVStore* kv, double value) {
    String* schemaStr = new String("D");
    Schema* newSchema = new Schema(schemaStr->c_str());
    delete schemaStr;
    DataFrame* newDf = new DataFrame(*newSchema);
    newDf->set(0, 0, value);
    kv->put(*key, newDf->serialize());
    return newDf;
  }
 
  /** Print the dataframe in SoR format to standard output. */
  void print() { 
    for (int i = 0; i < schema->length(); i++) {
      for (int j = 0; j < schema->width(); j++) {
        cout << "<";
        columns[j]->print(i);
        cout << ">";
      }
      if (i < schema->length()) {
        cout << endl;
      }
    }
  }

  bool equals(Object  * other) { 
    if (other == this) return true;
    DataFrame* x = dynamic_cast<DataFrame *>(other);
    if (x == nullptr) return false;
    if (!schema->equals(x->schema)) return false;
    for (int i = 0; i < ncols(); i++) {
      if (!columns[i]->equals(x->columns[i])) return false;
    }
    return true;
  }

  /** Convenience printing method for debugging. */
  void debug_print() {
    cout << endl << "-----------SCHEMA-----------" << endl;
    schema->debug_print();
    cout << endl << "---------END SCHEMA---------" << endl;
    cout << endl << "-----------SoR-----------" << endl;
    print();
    cout << endl << "---------END SoR---------" << endl;
  }

  unsigned char* serialize() { 
    size_t buffer_length = 100;
    unsigned char* serial = new unsigned char[buffer_length];
    unsigned char* schm = schema->serialize();
    size_t index = 16 + schema->width() + 1;
    copy_unsigned(serial, schm, index);
    for (size_t i = 0; i < schema->width(); i++) {
      if (schema->type(i) == 'S') {
        StringArray* stra = new StringArray(columns[i]);
        unsigned char* temp = stra->serialize();
        size_t length = extract_size_t(temp, 0);

        while (index + length >= buffer_length - 1) {
          unsigned char* temp = new unsigned char[buffer_length * 2];
          copy_unsigned(temp, serial, buffer_length);
          buffer_length *= 2;
          delete[] serial;
          serial = temp;
        }

        copy_unsigned(serial + index, temp, length);
        delete[] temp;
        index += length;
      } else {
        DoubleArray* dbl = new DoubleArray(columns[i]);
        unsigned char* temp = dbl->serialize();
        size_t length = extract_size_t(temp, 0);

        while (index + length >= buffer_length - 1) {
          unsigned char* temp = new unsigned char[buffer_length * 2];
          copy_unsigned(temp, serial, buffer_length);
          buffer_length *= 2;
          delete[] serial;
          serial = temp;
        }

        copy_unsigned(serial + index, temp, length);
        delete[] temp;
        index += length;
      }
    }
    return serial;
  }
  
  size_t deserialize(unsigned char* serialized) {
    schema = new Schema(serialized);
    col_cap = schema->col_cap;
    columns = new Column*[col_cap];
    size_t index = 16 + schema->width() + 1;

    DoubleArray* dbl;
    StringArray* str;

    for (int i = 0; i < schema->width(); i++) {
      switch (schema->type(i)) {
        case 'B':
          columns[i] = new BoolColumn();
          dbl = new DoubleArray();
          index += dbl->deserialize(serialized + index);
          for (size_t j = 0; j < dbl->len_; j++) {
            if (dbl->vals_[j] == 0) {
              set(i, j, false);
            } else {
              set(i, j, true);
            }
          }
          delete dbl;
          break;
        case 'I':
          columns[i] = new IntColumn();
          dbl = new DoubleArray();
          index += dbl->deserialize(serialized + index);
          for (size_t j = 0; j < dbl->len_; j++) {
            set(i, j, int(dbl->vals_[j]));
          }
          delete dbl;
          break;
        case 'S':
          columns[i] = new StringColumn();
          str = new StringArray();
          index += str->deserialize(serialized + index);
          for (size_t j = 0; j < str->len_; j++) {
            set(i, j, str->vals_[j]->clone());
          }
          delete str;
          break;
        case 'D':
          columns[i] = new DoubleColumn();
          dbl = new DoubleArray();
          index += dbl->deserialize(serialized + index);
          for (size_t j = 0; j < dbl->len_; j++) {
            set(i, j, dbl->vals_[j]);
          }
          delete dbl;
          break;
        default:
          assert("Type other than B, I, F, or S found." && false);
      }
    }
    return index;
  };

  ~DataFrame() {
    delete[] columns;
  }
};



//KVStore definitions///////////////////////
inline KVStore::KVStore() {}
inline KVStore::~KVStore() {}
inline bool KVStore::containsKey(Key* k) {
  return kv_map_.containsKey(k);
}
inline Value* KVStore::put(Key& k, Value* v) {
 return kv_map_.put(&k, v);
}
inline void KVStore::setApplication(Application* app) {
  app_ = app;
}
inline Value* KVStore::put(Key& k, unsigned char* data) {
  return put(k, new Value(data));
}
inline DataFrame* KVStore::get(Key& k) {
  // data is stored in local kvstore
  if (app_->this_node() == k.node_) {
    Value* v = kv_map_.get(&k);
    if (v != nullptr) {
      return new DataFrame(v->blob_);
    }
  }
  // ask the network for data
  Value* received = app_->requestValue(&k);
    if (received == nullptr) {
      return nullptr;
    }
  return new DataFrame(received->blob_);
}

inline Value* KVStore::waitAndGet(Key& k) {
    return nullptr;
}