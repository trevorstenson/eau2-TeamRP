//lang: CwC
#pragma once

#include "object.h"

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
public:
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) { }
 
  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) { }
  virtual void accept(double d) { }
  virtual void accept(int i) { }
  virtual void accept(String* s) { }
 
  /** Called when all fields have been seen. */
  virtual void done() { }
};

/**
 * Prints the fields
 * Authors:
 * Canon Sawrey sawrey.c@husky.neu.edu
 * Trevor Stenson stenson.t@husky.neu.edu */
class PrintFielder: public Fielder {
  public:
    size_t index;

    void start(size_t r) {
      index = r;
      std::cout << "|Printing row " << r << "|"; 
    }

    void accept(bool b) {
      std::cout << "<" << b << ">";
    }
    void accept(double d) { 
      std::cout << "<" << d << ">";
    }
    void accept(int i) {
      std::cout << "<" << i << ">";
    }
    void accept(String* s) {
      std::cout << "<" << s->c_str() << ">";
    }

    void done() { 
      std::cout << "|Finished printing row " << index << "|\n"; 
    }
};

class SorPrintFielder: public Fielder {
  public:
    size_t index;

    void accept(bool b) {
      std::cout << "<" << b << ">";
    }
    void accept(double d) { 
      std::cout << "<" << d << ">";
    }
    void accept(int i) {
      std::cout << "<" << i << ">";
    }
    void accept(String* s) {
      std::cout << "<" << s->c_str() << ">";
    }

    void done() {
      std::cout << "\n";
    }
};