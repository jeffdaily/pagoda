ACLOCAL_AMFLAGS = -I config

SUBDIRS = tests

HEADERS_PRIVATE = \
  AbstractVariable.H \
  Aggregation.H \
  AggregationUnion.H \
  Attribute.H \
  BoundaryVariable.H \
  ConnectivityVariable.H \
  CoordinateVariable.H \
  DataType.H \
  Dataset.H \
  Dimension.H \
  DistributedMask.H \
  FileWriter.H \
  LatLonBox.H \
  LocalMask.H \
  Mask.H \
  NetcdfAttribute.H \
  NetcdfDataset.H \
  NetcdfDimension.H \
  NetcdfFileWriter.H \
  NetcdfVariable.H \
  Pack.H \
  RangeException.H \
  Slice.H \
  StringComparator.H \
  SubsetterCommands.H \
  SubsetterException.H \
  TypedValues.H \
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
  DataType.C \
  Dataset.C \
  Dimension.C \
  DistributedMask.C \
  FileWriter.C \
  LatLonBox.C \
  LocalMask.C \
  Mask.C \
  NetcdfAttribute.C \
  NetcdfDataset.C \
  NetcdfDimension.C \
  NetcdfFileWriter.C \
  NetcdfVariable.C \
  Pack.C \
  StringComparator.C \
  SubsetterCommands.C \
  TypedValues.C \
  Util.C \
  Values.C \
  Variable.C \
  VariableDecorator.C

if WARNINGS
WALL = -Wall
endif

if PROFILE
PROFILE = -pg
endif

AM_CFLAGS = $(GA_INCS) $(WALL) $(PROFILE)
AM_CXXFLAGS = $(GA_INCS) $(CFLAGS) $(WALL) $(PROFILE)
AM_LDFLAGS = $(GA_LIBS) $(FLIBS) $(PROFILE)

lib_LTLIBRARIES = libsubsetter.la
libsubsetter_la_SOURCES = $(SOURCE) $(HEADERS_PRIVATE)

bin_PROGRAMS = subsetter
#subsetter_old_SOURCES = Mainp.C
#subsetter_old_LDADD = libsubsetter.la

subsetter_SOURCES = SubsetterMain.C
subsetter_LDADD = libsubsetter.la
