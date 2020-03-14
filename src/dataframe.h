//lang: CwC
#pragma once

#include "column.h"
#include "row.h"
#include "rower.h"
#include "schema.h"
#include <thread>
#include <mutex>
#include <functional>

#define MAX_THREADS 8
#define MIN_LINES 1000

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
class DataFrame : public Object {
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
    for (size_t i = 0; i < ncols(); i++) {
      String* temp = df.schema->col_names[i];
      if (temp != nullptr) {
        schema->col_names[i] = temp->clone();
      }
    } 
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
    for (size_t i = 0; i < ncols(); i++) {
      String* temp = schema->col_names[i];
      if (temp != nullptr) {
        schema->col_names[i] = temp->clone();
      }
      delete temp;
    }
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
    schema->ensure_length(addCol->size());
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
 
  /** Return the offset of the given column name or -1 if no such col. */
  int get_col(String& col) {
    char* characters = col.c_str();
    return schema->col_idx(characters);
  }
 
  /** Return the offset of the given row name or -1 if no such row. */
  int get_row(String& row) {
    char* characters = row.c_str();
    return schema->row_idx(characters);
  }
 
  /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
  void set(size_t col, size_t row, int val) {
    checkIndices(col, row, 'I');
    schema->ensure_length(row);
    IntColumn* column = columns[col]->as_int();
    if (!column) {
      assert("Unable to cast as IntColumn." && false);
    } else {
      column->set(row, val);
    }
  }
  void set(size_t col, size_t row, bool val) {
    checkIndices(col, row, 'B');
    schema->ensure_length(row);
    BoolColumn* column = columns[col]->as_bool();
    if (!column) {
      assert("Unable to cast as BoolColumn." && false);
    } else {
      column->set(row, val);
    }
  }
  void set(size_t col, size_t row, double val){
    checkIndices(col, row, 'D');
    schema->ensure_length(row);
    DoubleColumn* column = columns[col]->as_double();
    if (!column) {
      assert("Unable to cast as DoubleColumn." && false);
    } else {
      column->set(row, val);
    }
  }
  void set(size_t col, size_t row, String* val) {
    checkIndices(col, row, 'S');
    schema->ensure_length(row);
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
      schema->ensure_length(idx);
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

  /** Convenience printing method for debugging. */
  void debug_print() {
    cout << endl << "-----------SCHEMA-----------" << endl;
    schema->debug_print();
    cout << endl << "---------END SCHEMA---------" << endl;
    cout << endl << "-----------SoR-----------" << endl;
    print();
    cout << endl << "---------END SoR---------" << endl;
  }

  ~DataFrame() {
    delete[] columns;
  }
};