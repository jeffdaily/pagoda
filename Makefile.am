ACLOCAL_AMFLAGS = -I config

if WARNINGS
WALL = -Wall
endif

if PROFILE
PROFILE = -pg
endif

AM_CPPFLAGS = $(pnetcdf_CPPFLAGS) $(GA_CPPFLAGS)
AM_FFLAGS = $(WALL) $(PROFILE)
AM_CFLAGS = $(WALL) $(PROFILE)
AM_CXXFLAGS = $(WALL) $(PROFILE)
AM_LDFLAGS = $(PROFILE) $(pnetcdf_LDFLAGS) $(GA_LDFLAGS)
LDADD = libpagoda.la $(PNETCDF_LIBS) $(GA_LIBS) $(FLIBS)
LDADD_STANDALONE = $(PNETCDF_LIBS) $(GA_LIBS) $(FLIBS)

check_PROGRAMS = subsetter_old
bin_PROGRAMS = 
lib_LTLIBRARIES = libpagoda.la

subsetter_old_SOURCES = Mainp.C

libpagoda_la_SOURCES =    \
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
Array.C                   \
Array.H                   \
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
Error.H                   \
FileWriter.C              \
FileWriter.H              \
gax.H                     \
gax.C                     \
GlobalArray.C             \
GlobalArray.H             \
GlobalMask.C              \
GlobalMask.H              \
LatLonBox.C               \
LatLonBox.H               \
Mask.C                    \
Mask.H                    \
MaskMap.C                 \
MaskMap.H                 \
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
NotImplementedException.H \
pagoda.C                  \
pagoda.H                  \
Pack.C                    \
Pack.H                    \
Pnetcdf.C                 \
Pnetcdf.H                 \
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
check_PROGRAMS +=          \
tests/Test                 \
tests/TestAttribute        \
tests/TestBin              \
tests/TestDimension        \
tests/TestDistance         \
tests/TestEnum             \
tests/TestGet              \
tests/TestLocateRegion     \
tests/TestJoin             \
tests/TestNetcdfDataset    \
tests/TestOpen             \
tests/TestPartialSum       \
tests/TestRavel            \
tests/TestRead             \
tests/TestScanAdd          \
tests/TestSort             \
tests/TestUnion            \
tests/TestUnpack           \
tests/ga/Test              \
tests/ga/TestAttribute     \
tests/ga/TestBin           \
tests/ga/TestDimension     \
tests/ga/TestDistance      \
tests/ga/TestEnum          \
tests/ga/TestGet           \
tests/ga/TestLocateRegion  \
tests/ga/TestJoin          \
tests/ga/TestNetcdfDataset \
tests/ga/TestOpen          \
tests/ga/TestPartialSum    \
tests/ga/TestRavel         \
tests/ga/TestRead          \
tests/ga/TestScanAdd       \
tests/ga/TestSort          \
tests/ga/TestUnion         \
tests/ga/TestUnpack

tests_Test_SOURCES                 = tests/Test.C
tests_TestAttribute_SOURCES        = tests/TestAttribute.C
tests_TestBin_SOURCES              = tests/TestBin.C
tests_TestDimension_SOURCES        = tests/TestDimension.C
tests_TestDistance_SOURCES         = tests/TestDistance.C
tests_TestEnum_SOURCES             = tests/TestEnum.C
tests_TestGet_SOURCES              = tests/TestGet.C
tests_TestJoin_SOURCES             = tests/TestJoin.C
tests_TestLocateRegion_SOURCES     = tests/TestLocateRegion.C
tests_TestNetcdfDataset_SOURCES    = tests/TestNetcdfDataset.C
tests_TestOpen_SOURCES             = tests/TestOpen.C
tests_TestPartialSum_SOURCES       = tests/TestPartialSum.C
tests_TestRavel_SOURCES            = tests/TestRavel.C
tests_TestRead_SOURCES             = tests/TestRead.C
tests_TestScanAdd_SOURCES          = tests/TestScanAdd.C
tests_TestSort_SOURCES             = tests/TestSort.C
tests_TestUnion_SOURCES            = tests/TestUnion.C
tests_TestUnpack_SOURCES           = tests/TestUnpack.c
tests_ga_Test_SOURCES              = tests/ga/Test.C
tests_ga_TestAttribute_SOURCES     = tests/ga/TestAttribute.C
tests_ga_TestBin_SOURCES           = tests/ga/TestBin.C
tests_ga_TestDimension_SOURCES     = tests/ga/TestDimension.C
tests_ga_TestDistance_SOURCES      = tests/ga/TestDistance.C
tests_ga_TestEnum_SOURCES          = tests/ga/TestEnum.C
tests_ga_TestGet_SOURCES           = tests/ga/TestGet.C
tests_ga_TestJoin_SOURCES          = tests/ga/TestJoin.C
tests_ga_TestLocateRegion_SOURCES  = tests/ga/TestLocateRegion.C
tests_ga_TestNetcdfDataset_SOURCES = tests/ga/TestNetcdfDataset.C
tests_ga_TestOpen_SOURCES          = tests/ga/TestOpen.C
tests_ga_TestPartialSum_SOURCES    = tests/ga/TestPartialSum.C
tests_ga_TestRavel_SOURCES         = tests/ga/TestRavel.C
tests_ga_TestRead_SOURCES          = tests/ga/TestRead.C
tests_ga_TestScanAdd_SOURCES       = tests/ga/TestScanAdd.C
tests_ga_TestSort_SOURCES          = tests/ga/TestSort.C
tests_ga_TestUnion_SOURCES         = tests/ga/TestUnion.C
tests_ga_TestUnpack_SOURCES        = tests/ga/TestUnpack.c

tests_TestGet_LDADD    = $(LDADD_STANDALONE)
tests_ga_TestGet_LDADD = $(LDADD_STANDALONE)
