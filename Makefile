CXX := clang++
CXXFLAGS := -std=c++20 -Wall -Wextra -Wpedantic
SANITIZERS := -fsanitize=address,undefined -fno-omit-frame-pointer

.PHONY: test clean

main: main.cpp tick.h feed.h spsc_queue.h
	$(CXX) $(CXXFLAGS) -O0 -g main.cpp -o main

test: main.cpp tick.h feed.h spsc_queue.h
	$(CXX) $(CXXFLAGS) $(SANITIZERS) -O1 -g main.cpp -o main
	./main

clean:
	rm -rf main main.dSYM
