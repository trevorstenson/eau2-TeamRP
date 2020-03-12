build:
	g++ -g -pthread -std=c++11 test/sor_test.cpp -o sor_test

test:
	./sor_test

.PHONY: build test