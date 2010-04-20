ACLOCAL_AMFLAGS = -I config

if WARNINGS
WALL = -Wall
endif

if PROFILE
PROFILE = -pg
endif

AM_CPPFLAGS = $(pnetcdf_CPPFLAGS) $(ga_CPPFLAGS)
AM_FFLAGS = $(WALL) $(PROFILE)
AM_CFLAGS = $(WALL) $(PROFILE)
AM_CXXFLAGS = $(WALL) $(PROFILE)
AM_LDFLAGS = $(PROFILE) $(pnetcdf_LDFLAGS) $(ga_LDFLAGS)
LDADD = libsubsetter.la $(pnetcdf_LIBS) $(ga_LIBS) $(FLIBS)

check_PROGRAMS = subsetter_old
bin_PROGRAMS = subsetter
lib_LTLIBRARIES = libsubsetter.la

subsetter_old_SOURCES = Mainp.C

subsetter_SOURCES = SubsetterMain.C

libsubsetter_la_SOURCES = \
AbstractVariable.C        \
AbstractVariable.H        \
Aggregation.C             \
AggregationDimension.C    \
AggregationDimension.H    \
Aggregation.H             \
AggregationJoinExisting.C \
AggregationJoinExisting.H \
AggregationUnion.C        \
AggregationUnion.H        \
AggregationVariable.C     \
AggregationVariable.H     \
Attribute.C               \
Attribute.H               \
BoundaryVariable.C        \
BoundaryVariable.H        \
CommandLineParser.C       \
CommandLineParser.H       \
Common.H                  \
ConnectivityVariable.C    \
ConnectivityVariable.H    \
CoordinateVariable.C      \
CoordinateVariable.H      \
Dataset.C                 \
Dataset.H                 \
DataType.C                \
DataType.H                \
Debug.C                   \
Debug.H                   \
Dimension.C               \
Dimension.H               \
DistributedMask.C         \
DistributedMask.H         \
Error.H                   \
FileWriter.C              \
FileWriter.H              \
LatLonBox.C               \
LatLonBox.H               \
LocalMask.C               \
LocalMask.H               \
Mask.C                    \
Mask.H                    \
NetcdfAttribute.C         \
NetcdfAttribute.H         \
NetcdfDataset.C           \
NetcdfDataset.H           \
NetcdfDimension.C         \
NetcdfDimension.H         \
NetcdfError.H             \
NetcdfFileWriter.C        \
NetcdfFileWriter.H        \
NetcdfVariable.C          \
NetcdfVariable.H          \
Pack.C                    \
Pack.H                    \
PnetcdfTiming.C           \
PnetcdfTiming.H           \
RangeException.H          \
Slice.H                   \
StringComparator.C        \
StringComparator.H        \
SubsetterCommands.C       \
SubsetterCommands.H       \
SubsetterException.H      \
Timing.C                  \
Timing.H                  \
TypedValues.H             \
Util.C                    \
Util.H                    \
Values.C                  \
Values.H                  \
Variable.C                \
VariableDecorator.C       \
VariableDecorator.H       \
Variable.H

#############################################################################
# tests
#
check_PROGRAMS +=       \
tests/Test              \
tests/TestAttribute     \
tests/TestDimension     \
tests/TestDistance      \
tests/TestEnum          \
tests/TestGet           \
tests/TestLocateRegion  \
tests/TestJoin          \
tests/TestNetcdfDataset \
tests/TestOpen          \
tests/TestPartialSum    \
tests/TestRavel         \
tests/TestRead          \
tests/TestScanAdd       \
tests/TestUnion         \
tests/TestUnpack

tests_Test_SOURCES              = tests/Test.C
tests_TestAttribute_SOURCES     = tests/TestAttribute.C
tests_TestDimension_SOURCES     = tests/TestDimension.C
tests_TestDistance_SOURCES      = tests/TestDistance.C
tests_TestEnum_SOURCES          = tests/TestEnum.C
tests_TestGet_SOURCES           = tests/TestGet.C
tests_TestJoin_SOURCES          = tests/TestJoin.C
tests_TestLocateRegion_SOURCES  = tests/TestLocateRegion.C
tests_TestNetcdfDataset_SOURCES = tests/TestNetcdfDataset.C
tests_TestOpen_SOURCES          = tests/TestOpen.C
tests_TestPartialSum_SOURCES    = tests/TestPartialSum.C
tests_TestRavel_SOURCES         = tests/TestRavel.C
tests_TestRead_SOURCES          = tests/TestRead.C
tests_TestScanAdd_SOURCES       = tests/TestScanAdd.C
tests_TestUnion_SOURCES         = tests/TestUnion.C
tests_TestUnpack_SOURCES        = tests/TestUnpack.c
