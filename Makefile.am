ACLOCAL_AMFLAGS = -I config

HEADERS_PRIVATE = LatLonRange.H DimensionSlice64.H
SOURCE = Mainp.C LatLonRange.C DimensionSlice64.C

AM_CFLAGS = $(GA_INCS)
AM_CXXFLAGS = $(GA_INCS) $(CFLAGS)
#AM_LDFLAGS = $(GA_LIBS) $(FCLIBS)
AM_LDFLAGS = $(GA_LIBS) $(FLIBS)

bin_PROGRAMS = psubsetter
psubsetter_SOURCES = $(SOURCE) $(HEADERS_PRIVATE)

check_PROGRAMS = r12_smaller
r12_smaller_SOURCES = r12_smaller.c
r12_smaller_CFLAGS = -I/home/d3n000/gcrm/local/include
r12_smaller_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf
