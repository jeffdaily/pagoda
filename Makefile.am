ACLOCAL_AMFLAGS = -I config

SUBDIRS = tests

HEADERS_PRIVATE = \
AbstractVariable.H \
AggregationDimension.H \
Aggregation.H \
AggregationJoinExisting.H \
AggregationUnion.H \
AggregationVariable.H \
Attribute.H \
BoundaryVariable.H \
CommandLineParser.H \
Common.H \
ConnectivityVariable.H \
CoordinateVariable.H \
Dataset.H \
DataType.H \
Debug.H \
Dimension.H \
DistributedMask.H \
Error.H \
FileWriter.H \
LatLonBox.H \
LocalMask.H \
Mask.H \
NetcdfAttribute.H \
NetcdfDataset.H \
NetcdfDimension.H \
NetcdfError.H \
NetcdfFileWriter.H \
NetcdfVariable.H \
Pack.H \
PnetcdfTiming.H \
RangeException.H \
Slice.H \
StringComparator.H \
SubsetterCommands.H \
SubsetterException.H \
Timing.H \
TypedValues.H \
Util.H \
Values.H \
VariableDecorator.H \
Variable.H

SOURCE = \
AbstractVariable.C \
Aggregation.C \
AggregationDimension.C \
AggregationJoinExisting.C \
AggregationUnion.C \
AggregationVariable.C \
Attribute.C \
BoundaryVariable.C \
CommandLineParser.C \
ConnectivityVariable.C \
CoordinateVariable.C \
Dataset.C \
DataType.C \
Debug.C \
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
PnetcdfTiming.C \
StringComparator.C \
SubsetterCommands.C \
Timing.C \
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
