#include "application.h"
#include "../dataframe/dataframe.h"
#include "../dataframe/visitor.h"
#include "../dataframe/rower.h"
//#include <../map.h>
#include "assert.h"
#include <cstring>
#include <iostream>
#include <stdio.h>

class FileReader : public Visitor {
public:
  /** Reads next word and stores it in the row. Actually read the word.
      While reading the word, we may have to re-fill the buffer  */
    void visit(Row & r) override {
        assert(i_ < end_);
        assert(! isspace(buf_[i_]));
        size_t wStart = i_;
        while (true) {
            if (i_ == end_) {
                if (feof(file_)) { ++i_;  break; }
                i_ = wStart;
                wStart = 0;
                fillBuffer_();
            }
            if (isspace(buf_[i_]))  break;
            ++i_;
        }
        buf_[i_] = 0;
        String word(buf_ + wStart, i_ - wStart);
        r.set(0, &word);
        ++i_;
        skipWhitespace_();
    }
 
    /** Returns true when there are no more words to read.  There is nothing
       more to read if we are at the end of the buffer and the file has
       all been read.     */
    bool done() override { return (i_ >= end_) && feof(file_);  }
 
    /** Creates the reader and opens the file for reading.  */
    FileReader() {
        file_ = fopen("test/wordcount.txt", "r");
        if (file_ == nullptr) assert("Cannot open file wordcount.txt");
        buf_ = new char[BUFSIZE + 1]; //  null terminator
        fillBuffer_();
        skipWhitespace_();
    }
 
    static const size_t BUFSIZE = 1024;
 
    /** Reads more data from the file. */
    void fillBuffer_() {
        size_t start = 0;
        // compact unprocessed stream
        if (i_ != end_) {
            start = end_ - i_;
            memcpy(buf_, buf_ + i_, start);
        }
        // read more contents
        end_ = start + fread(buf_+start, sizeof(char), BUFSIZE - start, file_);
        i_ = start;
    }
 
    /** Skips spaces.  Note that this may need to fill the buffer if the
        last character of the buffer is space itself.  */
    void skipWhitespace_() {
        while (true) {
            if (i_ == end_) {
                if (feof(file_)) return;
                fillBuffer_();
            }
            // if the current character is not whitespace, we are done
            if (!isspace(buf_[i_]))
                return;
            // otherwise skip it
            ++i_;
        }
    }
 
    char * buf_;
    size_t end_ = 0;
    size_t i_ = 0;
    FILE * file_;
};
 
 
/****************************************************************************/
class Adder : public Rower {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  Adder(SIMap& map) : map_(map)  {}
 
  bool accept(Row& r) override {
    String* word = r.get_string(0);
    assert(word != nullptr);
    Integer* num = map_.containsKey(word) ? map_.get(word) : new Integer();
    assert(num != nullptr);
    num->val_++;
    map_.set(*word, num);
    return false;
  }
};
 
/***************************************************************************/
class Summer : public Visitor {
public:
  SIMap& map_;
  size_t i = 0;
  size_t j = 0;
  size_t seen = 0;
 
  Summer(SIMap& map) : map_(map) {}
 
  void next() {
      if (i == map_.capacity_ ) return;
      if ( j < map_.size() ) {
          j++;
          ++seen;
      } else {
          ++i;
          j = 0;
          while( i < map_.capacity_ && map_.size() == 0 )  i++;
          if (k()) ++seen;
      }
  }
 
  String* k() {
      if (i==map_.capacity_ || j == map_.size()) return nullptr;
      return (String*)(map_.values_[i]->getKey());
  }
 
  size_t v() {
      if (i == map_.capacity_ || j == map_.size()) {
          assert(false); return 0;
      }
      return ((size_t)(dynamic_cast<Integer*>(map_.values_[i]->getValue()))->get());
  }
 
  void visit(Row& r) {
      if (!k()) next();
      String & key = *k();
      size_t value = v();
      r.set(0, &key);
      r.set(1, (int) value);
      next();
  }
 
  bool done() {return seen == map_.size(); }
};
 
/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount : public Application {
public:
  static const size_t BUFSIZE = 1024;
  Key in;
  KeyBuff kbuf;
  SIMap all;
 
  WordCount(size_t idx):
    Application(idx), in("data"), kbuf(new Key("wc-map-",0)) { }
 
  /** The master nodes reads the input, then all of the nodes count. */
  void run_() override {
    if (idx_ == 0) {
      FileReader fr;
      delete DataFrame::fromVisitor(&in, &kv, "S", &fr);
    }
    local_count();
    reduce();
  }
 
  /** Returns a key for given node.  These keys are homed on master node
   *  which then joins them one by one. */
  Key* mk_key(size_t idx) {
      Key * k = kbuf.c(idx)->get();
      //printf("Created key %s", k->c_str());
      return k;
  }
 
  /** Compute word counts on the local node and build a data frame. */
  void local_count() {
    DataFrame* words = (kv.waitAndGet(in));
    p("Node ").p(idx_).pln(": starting local count...");
    SIMap map;
    Adder add(map);
    //words->localmap(add);
    words->map(add);
    delete words;
    Summer cnt(map);
    delete DataFrame::fromVisitor(mk_key(idx_), &kv, "SI", &cnt);
    std::cout << "1" << endl << std::flush;
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (idx_ != 0) return;
    pln("Node 0: reducing counts...");
    SIMap map;
    Key* own = mk_key(0);
    std::cout << "2" << endl << std::flush;
    merge(kv.get(*own), map);
    std::cout << "2" << endl << std::flush;
    for (size_t i = 1; i < 3; ++i) { // merge other nodes
      Key* ok = mk_key(i);
      merge(kv.waitAndGet(*ok), map);
      delete ok;
    }
    p("Different words: ").pln(map.size());
    delete own;
  }
 
  void merge(DataFrame* df, SIMap& m) {
    Adder add(m);
    df->map(add);
    delete df;
  }
}; // WordcountDemo