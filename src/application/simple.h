#include "application.h"
#include "../dataframe/dataframe.h"

class Simple : public Application {
public:
 
  Simple(size_t idx): Application(idx) { }
 
  void run_() override {
    switch(this_node()) {
        case 0:   put_sender();     break;
        case 1:   receiver();      break;
        case 2:   others();
    }
  }

  void put_sender() {
    usleep(4000000);
    Key* k = new Key("testing", 1);
    unsigned char testStr[] = "hello there";
    unsigned char* testBuffer = &testStr[0];
    Value* v = new Value(testBuffer, strlen((char*)testBuffer));
    kv.put(*k, v);
  }

  void receiver() {
    usleep(6000000);
    printf("kvmap in node 1:\n");
    kv.kv_map_.print();
  }

  void others() {

  }
};