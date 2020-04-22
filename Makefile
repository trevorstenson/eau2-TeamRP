linus:
	g++ -g -pthread -w -std=c++11 test/milestone5.cpp -o m5
	./m5

wordcount:
	g++ -g -pthread -w -std=c++11 test/milestone4.cpp -o m4	
	cp test/wordcount.txt .
	./m4
	
distributed:
	g++ -g -pthread -w  -std=c++11 test/distributed.cpp -o distributed
	./distributed

valgrind:
	valgrind --leak-check=full ./m5

previous_milestones:
	g++ -g -pthread -w -std=c++11 test/milestone2.cpp -o m2
	./m2
	#g++ -g -pthread -w -std=c++11 test/milestone3.cpp -o m3
	#NOTE: M3 can no longer run, as mock networking functionality has been removed.
	g++ -g -pthread -w -std=c++11 test/milestone4.cpp -o m4	
	cp test/wordcount.txt .
	./m4

test:
	#Compiling and generating test files. 
	python3 gen.py
	g++ -g -pthread -w -std=c++11 test/serial_test.cpp -o serial_test
	g++ -g -pthread -w -std=c++11 test/util_test.cpp -o util_test
	g++ -g -pthread -w -std=c++11 test/sor_test.cpp -o sor_test
	g++ -g -pthread -w -std=c++11 test/dataframe_test.cpp -o dataframe_test
	#Running tests
	./serial_test
	./util_test
	./sor_test
	./dataframe_test

valgrind:
	valgrind --leak-check=full ./serial_test
	valgrind --leak-check=full ./util_test
	valgrind --leak-check=full ./sor_test
	valgrind --leak-check=full ./dataframe_test

clean:
	rm *.sor || true
	rm -r *.dSYM || true
	rm ./serial_test || true
	rm ./util_test || true
	rm ./sor_test || true
	rm ./dataframe_test || true
	rm ./m2 || true
	rm ./m3 || true
	rm ./m4 || true
	rm ./m5 || true
	rm wordcount.txt || true
	rm ./distributed || true
	
.PHONY: build test clean distributed