build:
	python3 gen.py
	g++ -g -pthread -std=c++11 test/util_test.cpp -o util_test
	g++ -g -pthread -std=c++11 test/sor_test.cpp -o sor_test

test:
	./util_test
	./sor_test

clean:
	rm ./util_test
	rm ./sor_test
	rm *.sor
	
.PHONY: build test clean