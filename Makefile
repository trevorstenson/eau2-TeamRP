build:
	python3 test/gen.py
	g++ -g -pthread -std=c++11 test/util_test.cpp -o util_test
	g++ -g -pthread -std=c++11 test/sor_test.cpp -o sor_test
	g++ -g -pthread -std=c++11 test/df_test.cpp -o df_test

test:
	./util_test
	./sor_test
	./df_test

valgrind:
	valgrind --leak-check=full ./df_test

clean:
	rm ./util_test
	rm ./sor_test
	rm ./df_test
	rm /test/*.sor
	
.PHONY: build test clean