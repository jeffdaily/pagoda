GA_INCLUDE=-I/home/d3n000/gcrm/ga/ga-4-1-1/include
GA_LIB_STATIC=-L/home/d3n000/gcrm/ga/ga-4-1-1/lib/LINUX
GA_LIB_SHARED=-L/home/d3n000/gcrm/ga/ga-4-1-1/lib/LINUX/shared
GA_LIBS=-lglobal -lma -llinalg -larmci -lm

PINC=-I/home/d3n000/gcrm/local/include $(GA_INCLUDE)
PLIB=-L/home/d3n000/gcrm/local/lib -lpnetcdf $(GA_LIB_STATIC) $(GA_LIBS)

INC=-I/home/d3n000/gcrm/local/include
LIB=-L/home/d3n000/gcrm/local/lib -lnetcdf_c++ -lnetcdf

MPICC=/home/d3n000/gcrm/local/bin/mpicc
MPICXX=/home/d3n000/gcrm/local/bin/mpicxx
MPIFC=/home/d3n000/gcrm/local/bin/mpif77
CXX=g++

FFLAGS=
CFLAGS=-g -O2 -Wall -Wno-extra
CXXFLAGS=-g -O2 -Wall -Wno-extra

POBJ=Mainp.o LatLonRange.o DimensionSlice64.o
OBJ=Main.C LatLonRange.C DimensionSlice.C

all: psubsetter subsetter

psubsetter: $(POBJ)
	$(MPICXX) $(DEFS) $(CXXFLAGS) $(POBJ) $(PLIB) -lg2c -o $@

subsetter: $(OBJ)
	$(MPICXX) $(DEFS) $(CXXFLAGS) $(OBJ) $(INC) $(LIB) -o $@

%.o : %.c
	$(MPICC) $(DEFS) $(CFLAGS) $(PINC) -c $< -o $@

%.o : %.C
	$(MPICXX) $(DEFS) $(CXXFLAGS) $(PINC) -c $< -o $@

clean:
	rm -f *.o subsetter psubsetter
