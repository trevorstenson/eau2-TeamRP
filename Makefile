build:
	#python3 gen.py
	#cp test/*.sor .
	#g++ -g -pthread -std=c++11 test/util_test.cpp -o util_test
	#g++ -g -pthread -std=c++11 test/sor_test.cpp -o sor_test
	#g++ -g -pthread -std=c++11 test/df_test.cpp -o df_test
	g++ -g -pthread -std=c++11 test/milestone2.cpp -o m2

test:
	#./util_test
	#./sor_test
	#./df_test
	./m2

valgrind:
	valgrind --leak-check=full ./df_test

clean:
	rm ./util_test
	rm ./sor_test
	rm ./df_test
	rm ./m2
	rm test/*.sor
	rm ./*.sor
	
.PHONY: build test clean