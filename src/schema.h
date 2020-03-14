//lang: CwC
#pragma once

#include "string.h"


/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 * Authors:
 * Canon Sawrey   sawrey.c@husky.neu.edu
 * Trevor Stenson stenson.t@husky.neu.edu
 */

using namespace std;

class Schema : public Object {
  public:
    size_t n_col;
    size_t n_row;
    size_t col_cap;
    size_t row_cap;
    char* types;
    String** col_names;
    String** row_names;


    /** Copying constructor */
    Schema(Schema& from) {
      n_col = from.n_col;
      n_row = from.n_row;
      col_cap = from.col_cap;
      row_cap = from.row_cap;
      types = new char[col_cap];
      strcpy(types, from.types);
      col_names = new String*[col_cap];
      row_names = new String*[row_cap];
      for (int i = 0; i < n_col; i++) {
        col_names[i] = from.col_name(i);
      }
      for (int i = 0; i < n_row; i++) {
        row_names[i] = from.row_name(i);
      }
    }
  
    /** Create an empty schema **/
    Schema() {
      n_col = 0;
      n_row = 0;
      col_cap = 4;
      types = new char[col_cap];
      col_names = new String*[col_cap];
      row_cap = 4;
      row_names = new String*[row_cap];
    }
  
    /** Create a schema from a string of types. A string that contains
      * characters other than those identifying the four type results in
      * undefined behavior. The argument is external, a nullptr argument is
      * undefined. **/
    Schema(const char* types_) {
      if (containsValidTypes(types_)) {
        n_col = strlen(types_);
        size_t capacity = n_col < 4 ? 4 : n_col;
        col_cap = capacity;
        types = new char[col_cap];
        strcpy(types, types_);
        col_names = new String*[col_cap];
        for (int i = 0; i < col_cap; i++) {
          col_names[i] = nullptr;
        }
        n_row = 0;
        row_cap = 4;
        row_names = new String*[row_cap];
        for (int i = 0; i < row_cap; i++) {
          row_names[i] = nullptr;
        }
      } else {
        assert("Cannot instantiate types other than B, I, D, or S." && false);
      }
    }

    /** Returns char representing column type at idx.
     * Invalid idx exits */
    char type(size_t idx) {
      if (idx >= strlen(types)) {
        assert("Index out of bounds." && false);
      }
      return types[idx];
    }

    /** Ensures the char* contains only B, I, F, and S, representing
     *  boolean, integer, float, and string types. **/
    bool containsValidTypes(const char* types) {
      for (int i = 0; i < strlen(types); i++) {
        if (!isValidType(types[i])) {
          return false;
        }
      }
      return true;
    }

     /** Ensures the char is one of B, I, F, and S, representing
     *  boolean, integer, float, or string type. **/
    bool isValidType(const char type) {
      return (type == 'B' || type == 'I' || type == 'D' || type == 'S');
    }
  
    /** Add a column of the given type and name (can be nullptr), name
      * is external. Names are expectd to be unique, duplicates result
      * in undefined behavior. */
    void add_column(char typ, String* name) {
      if (!isValidType(typ)) {
        assert("Cannot instantiate types other than B, I, D, or S." && false);
      }
      ensureColumnCapacity();
      types[n_col] = typ;
      if (name != nullptr) {
        col_names[n_col] = name->clone();
      } else {
        col_names[n_col] = nullptr;
      }
      n_col++;
    }

    /** Ensures the current column capacity is enough to
     * accomodate an addition */
    void ensureColumnCapacity() {
      if (n_col == col_cap) {
        growColumns();
      }
    }

    /** Increase columm capacity and copies over old values */
    void growColumns() {
      if (col_cap == 0) {
        col_cap++;
      } else {
        col_cap *= 2;
      }
      char* temp = types;
      types = new char[col_cap];
      strcpy(types, temp);
      String** newValues = new String*[col_cap];
      for (size_t i = 0; i < n_col; i++) {
        newValues[i] = col_names[i];
      }
      delete[] col_names;
      col_names = newValues;
    }

  
    /** Add a row with a name (possibly nullptr), name is external.  Names are
     *  expectd to be unique, duplicates result in undefined behavior. */
    void add_row(String* name) { 
      ensureRowCapacity();
      if (name != nullptr) {
        row_names[n_row] = name->clone();
      } else {
        row_names[n_row] = nullptr;
      }
      n_row++;
    }


    /** Ensures the current row capacity is enough to
     * accomodate an addition */
    void ensureRowCapacity() {
      if (n_row == row_cap) {
        growRows();
      }
    }

    /** Increase row capacity and copies over old values */
    void growRows() {
      if (row_cap == 0) {
        row_cap++;
      } else {
        row_cap *= 2;
      }
      String** newValues = new String*[row_cap];
      for (size_t i = 0; i < n_row; i++) {
        newValues[i] = row_names[i];
      }
      delete[] row_names;
      row_names = newValues;
    }
  
    /** Return name of row at idx; nullptr indicates no name. An idx >= width
      * is undefined. */
    String* row_name(size_t idx) {
      if (idx >= n_row) {
        assert("Index out of bounds." && false);
      } else {
        return row_names[idx];
      } 
    }
  
    /** Return name of column at idx; nullptr indicates no name given.
      *  An idx >= width is undefined.*/
    String* col_name(size_t idx) { 
      if (idx >= n_col) {
        assert("Index out of bounds." && false);
      } else {
        return col_names[idx];
      }
    }
  
    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) { 
      if (idx >= n_col) {
        assert("Index out of bounds." && false);
      } else {
        return types[idx];
      }
    }
  
    /** Given a column name return its index, or -1. */
    int col_idx(const char* name) {
      String* str = new String(name);
      for (size_t i = 0; i < n_col; i++) {
        if (str->equals(col_names[i])) {
          return i;
        }
      }
      return -1;
    }
  
    /** Given a row name return its index, or -1. */
    int row_idx(const char* name) {
      String* str = new String(name);
      for (size_t i = 0; i < n_row; i++) {
        if (str->equals(row_names[i])) {
          return i;
        }
      }
      return -1;
    }
  
    /** The number of columns */
    size_t width() {
      return n_col;
    }
  
    /** The number of rows */
    size_t length() {
      return n_row;
    }

    /** Convenience printing method for debugging. */
    void debug_print() {
      cout << endl << "n_col: " << n_col << endl;
      cout << "n_row: " << n_row << endl;
      cout << "types: " << types << endl;
      cout << "col_cap: " << col_cap << endl;
      cout << "row_cap: " << row_cap << endl;
      cout << "col_names:\n";
      for (int i = 0; i < n_col; i++) {
        String* str = col_names[i];
        if (str == nullptr) {
          cout << pad_string("[NONE]", 10);
        } else {
          cout << pad_string(str->c_str(), 10);
        }
      }
      cout << "\nrow_names:\n";
      for (int i = 0; i < n_row; i++) {
        String* str = row_names[i];
        if (str == nullptr) {
          cout << pad_string("[NONE]", 10);
        } else {
          cout << pad_string(str->c_str(), 10);
        }
      }
    }

    ~Schema() {
      delete[] types;
      for (size_t i = 0; i < n_col; i++) {
        delete col_names[i];
      }
      for (size_t j = 0; j < n_row; j++) {
        delete row_names[j];
      }
      delete[] col_names;
      delete[] row_names;
    }

    /**
     * Method to ensure the schema recognizes length as the schema's length
     * @param length the length of the ne
     */
    void ensure_length(size_t row) {
      for (int i = n_row; i <= row; i++) {
        add_row(nullptr);
      }
    }

    /**
     * Pads or slices the input string to the input size
     * @param s The string to be padded of shortened
     * @param l The desired size
     */
    char* pad_string(const char* s, size_t l) {
      char* ret_pointer = new char[l];
      for (int i = 0; i < l; i++) {
        if (i < strlen(s)) {
          ret_pointer[i] = s[i];
        } else {
          ret_pointer[i] = ' ';
        }
      }
      return ret_pointer;
    }
};