ACLOCAL_AMFLAGS = -I config

HEADERS_PRIVATE = \
  AbstractVariable.H \
  Aggregation.H \
  AggregationUnion.H \
  Attribute.H \
  BoundaryVariable.H \
  ConnectivityVariable.H \
  CoordinateVariable.H \
  Dataset.H \
  DataType.H \
  Dimension.H \
  DimensionSlice.H \
  Mask.H \
  NetcdfAttribute.H \
  NetcdfDataset.H \
  NetcdfDimension.H \
  NetcdfFileWriter.H \
  NetcdfVariable.H \
  LatLonBox.H \
  RangeException.H \
  Slice.H \
  StringComparator.H \
  SubsetterCommands.H \
  SubsetterException.H \
  Util.H \
  Values.H \
  Variable.H \
  VariableDecorator.H

SOURCE = \
  AbstractVariable.C \
  Aggregation.C \
  AggregationUnion.C \
  Attribute.C \
  BoundaryVariable.C \
  ConnectivityVariable.C \
  CoordinateVariable.C \
  Dataset.C \
  DataType.C \
  Dimension.C \
  LatLonBox.C \
  Mask.C \
  NetcdfAttribute.C \
  NetcdfDataset.C \
  NetcdfDimension.C \
  NetcdfFileWriter.C \
  NetcdfVariable.C \
  StringComparator.C \
  SubsetterCommands.C \
  Util.C \
  Values.C \
  Variable.C \
  VariableDecorator.C

AM_CFLAGS = $(GA_INCS)
AM_CXXFLAGS = $(GA_INCS) $(CFLAGS)
#AM_LDFLAGS = $(GA_LIBS) $(FCLIBS)
AM_LDFLAGS = $(GA_LIBS) $(FLIBS)

lib_LTLIBRARIES = libsubsetter.la
libsubsetter_la_SOURCES = $(SOURCE) $(HEADERS_PRIVATE)

bin_PROGRAMS = subsetter_old subsetter
subsetter_old_SOURCES = Mainp.C
subsetter_old_LDADD = libsubsetter.la

subsetter_SOURCES = SubsetterMain.C
subsetter_LDADD = libsubsetter.la

check_PROGRAMS = \
  r12_smaller \
  Test \
  TestOpen \
  TestAttribute \
  TestDimension \
  TestNetcdfDataset \
  TestUnion \
  TestRavel

r12_smaller_SOURCES = r12_smaller.c
r12_smaller_CFLAGS = -I/home/d3n000/gcrm/local/include
r12_smaller_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf

Test_SOURCES = Test.C
Test_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
Test_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)

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

TestNetcdfDataset_SOURCES = TestNetcdfDataset.C
TestNetcdfDataset_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestNetcdfDataset_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestNetcdfDataset_LDADD = libsubsetter.la

TestUnion_SOURCES = TestUnion.C
TestUnion_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestUnion_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestUnion_LDADD = libsubsetter.la

TestRavel_SOURCES = TestRavel.C
TestRavel_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestRavel_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestRavel_LDADD = libsubsetter.la

