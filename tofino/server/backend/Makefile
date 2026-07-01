CXX = g++

# -Wshadow=local
CXXFLAGS = -std=c++17 -Wall -Wextra -Wshadow -Wno-missing-field-initializers -Wno-unused-result -Wno-unused-function -O3 -I../libs/argparse/include -I../libs/spdlog/include -DRTE_MEMCPY_AVX512 $(shell pkg-config --cflags libdpdk) -march=native -mavx512f -mavx512bw
LINKFLAGS = $(shell pkg-config --libs libdpdk) -lpthread -lnuma -lrt # shm_open requires librt

BUILDDIR = build
TARGET = main

SRCS = $(wildcard *.cpp) 
INCS = $(wildcard *.h) $(wildcard *.hpp) $(wildcard ../common/*.h) $(wildcard ../common/*.hpp)
OBJS = $(SRCS:%.cpp=$(BUILDDIR)/%.o)

all: $(BUILDDIR)/$(TARGET)

$(BUILDDIR)/$(TARGET): $(OBJS)
	$(CXX) -o $@ $^ $(LINKFLAGS)

$(BUILDDIR)/%.o: %.cpp $(INCS) Makefile | $(BUILDDIR)
	$(CXX) -c -o $@ $< $(CXXFLAGS) 

$(BUILDDIR): 
	mkdir $(BUILDDIR)

clean:
	rm -r $(BUILDDIR)/* 
