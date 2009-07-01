ACLOCAL_AMFLAGS = -I config

HEADERS_PRIVATE = \
  Attribute.H \
  CommandLineParser.H \
  Dataset.H \
  DataType.H \
  Dimension.H \
  DimensionSlice.H \
  NetcdfAttribute.H \
  NetcdfDataset.H \
  NetcdfDimension.H \
  NetcdfVariable.H \
  LatLonBox.H \
  RangeException.H \
  Slice.H \
  SubsetterException.H \
  Util.H \
  Values.H \
  Variable.H

SOURCE = \
  Attribute.C \
  CommandLineParser.C \
  Dataset.C \
  DataType.C \
  Dimension.C \
  LatLonBox.C \
  NetcdfAttribute.C \
  NetcdfDataset.C \
  NetcdfDimension.C \
  NetcdfVariable.C \
  Util.C \
  Values.C \
  Variable.C

AM_CFLAGS = $(GA_INCS)
AM_CXXFLAGS = $(GA_INCS) $(CFLAGS)
#AM_LDFLAGS = $(GA_LIBS) $(FCLIBS)
AM_LDFLAGS = $(GA_LIBS) $(FLIBS)

lib_LTLIBRARIES = libsubsetter.la
libsubsetter_la_SOURCES = $(SOURCE) $(HEADERS_PRIVATE)

bin_PROGRAMS = psubsetter
psubsetter_SOURCES = Mainp.C
psubsetter_LDADD = libsubsetter.la

check_PROGRAMS = r12_smaller TestOpen TestAttribute TestDimension
r12_smaller_SOURCES = r12_smaller.c
r12_smaller_CFLAGS = -I/home/d3n000/gcrm/local/include
r12_smaller_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf

TestOpen_SOURCES = TestOpen.C
TestOpen_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestOpen_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)

TestAttribute_SOURCES = TestAttribute.C
TestAttribute_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestAttribute_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestAttribute_LDADD = libsubsetter.la

TestDimension_SOURCES = TestDimension.C
TestDimension_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestDimension_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestDimension_LDADD = libsubsetter.la

