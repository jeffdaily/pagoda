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
  Dataset.H \
  DataType.H \
  Dimension.H \
  DimensionSlice.H \
  LatLonBox.H \
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
  Pack.C \
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

