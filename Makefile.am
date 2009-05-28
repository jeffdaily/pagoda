ACLOCAL_AMFLAGS = -I config

HEADERS_PRIVATE = LatLonRange.H DimensionSlice64.H
SOURCE = Mainp.C LatLonRange.C DimensionSlice64.C

AM_CFLAGS = $(GA_INCS)
AM_CXXFLAGS = $(GA_INCS)
AM_LDFLAGS = $(GA_LIBS) $(FCLIBS)

bin_PROGRAMS = psubsetter
psubsetter_SOURCES = $(SOURCE) $(HEADERS_PRIVATE)
