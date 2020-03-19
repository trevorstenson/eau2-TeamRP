//lang: CwC
#pragma once

#include "string.h"
#include "serial/src/serial.h"

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

class Schema : public Object, public Serializable {
  public:
    size_t n_col;
    size_t n_row;
    size_t col_cap;
    char* types;


    /** Copying constructor */
    Schema(Schema& from) {
      n_col = from.n_col;
      n_row = from.n_row;
      col_cap = from.col_cap;
      types = new char[col_cap];
      strcpy(types, from.types);
    }
  
    /** Create an empty schema **/
    Schema() {
      n_col = 0;
      n_row = 0;
      col_cap = 4;
      types = new char[col_cap];
    }

    Schema(unsigned char* serial): Schema() {
      deserialize(serial);
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
        n_row = 0;
      } else {
        assert("Cannot instantiate types other than B, I, D, or S." && false);
      }
    }

    /** Returns char representing column type at idx.
     * Invalid idx exits */
    char type(size_t idx) {
      if (idx >= col_cap) {
        assert("Index out of bounds." && false);
      }
      return types[idx];
    }

    /** Ensures the char* contains only B, I, F, and S, representing
     *  boolean, integer, double, and string types. **/
    bool containsValidTypes(const char* types) {
      for (int i = 0; i < strlen(types); i++) {
        if (!isValidType(types[i])) {
          return false;
        }
      }
      return true;
    }

     /** Ensures the char is one of B, I, F, and S, representing
     *  boolean, integer, double, or string type. **/
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
    }
  
    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) { 
      if (idx >= n_col) {
        assert("Index out of bounds." && false);
      } else {
        return types[idx];
      }
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
    }

    ~Schema() {
      delete[] types;
    }

    /**
     * Method to ensure the schema recognizes length as the schema's length
     * @param length the length of the ne
     */
    void new_length(size_t row) {
      if (row >= length()) n_row = row + 1;
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

    bool equals(Object  * other) { 
      if (other == this) return true;
      Schema* x = dynamic_cast<Schema *>(other);
      if (x == nullptr) return false;
      if (width() != x->width() || length() != x->length()) return false;
      return (strcmp(types, x->types) == 0);
    }

    /** Serializes the DoubleArray into an unsigned char array. Structure is as following
         * 
         * |--8 bytes--|--8 bytes-----|---Unknown----|
         * |--n_row----|--n_col-------|---types------|
    */
    unsigned char* serialize() { 
      unsigned char* buffer = new unsigned char[16 + width() + 1];
      unsigned char* cast = reinterpret_cast<unsigned char*>(types);
      insert_size_t(n_row, buffer, 0);
      insert_size_t(n_col, buffer, 8);
      insert_char_arr(types, buffer, 16, true);
      return buffer;
    }

    size_t deserialize(unsigned char* serialized) { 
      n_row = extract_size_t(serialized, 0);
      size_t width = extract_size_t(serialized, 8);
      char* temp = reinterpret_cast<char*>(serialized + 16);
      for (size_t i = 0; i < width; i++) {
        add_column(temp[i], nullptr);
      }
      assert(n_col == width);
      return 16 + width + 1;
    }
};