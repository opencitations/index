CXX = g++ -std=c++17
CXXFLAGS = -O3 -Wall -ftree-vectorize -lpthread
INCLUDE = -I ./index/cpp/include/ -I ./index/cpp/lib/

all:
	@echo "Please choose one of the following target: build, install, clean"
	@exit 2

build:
	mkdir -p ./bin
	@echo "Compiling build.cpp"
	$(CXX) ./index/cpp/src/build.cpp -o ./bin/oci_moph $(CXXFLAGS) $(INCLUDE) -lzip
	@echo "Compiling lookup.cpp"
	$(CXX) ./index/cpp/src/lookup.cpp -o ./bin/oci_lookup $(CXXFLAGS) $(INCLUDE) -lzip

build-experimental:
	mkdir -p ./bin
	@echo "Compiling build.cpp"
	$(CXX) ./index/cpp/src/build.cpp -o ./bin/oci_moph $(CXXFLAGS) $(INCLUDE) -lzip -lstdc++fs
	@echo "Compiling lookup.cpp"
	$(CXX) ./index/cpp/src/lookup.cpp -o ./bin/oci_lookup $(CXXFLAGS) $(INCLUDE) -lzip -lstdc++fs

install:
	@echo "Installing oci_moph"
	cp ./bin/oci_moph /usr/local/bin/oci_moph
	cp ./bin/oci_lookup /usr/local/bin/oci_lookup

.PHONY: clean
clean: 
	@echo "Deleting build"
	rm -r ./bin
	@echo "Build deleted"