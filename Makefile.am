ACLOCAL_AMFLAGS = -I config

if WARNINGS
WALL = -Wall
endif

if PROFILE
PROFILE = -pg
endif

AM_CPPFLAGS = -I$(top_srcdir)/src $(pnetcdf_CPPFLAGS) $(GA_CPPFLAGS)
AM_FFLAGS = $(WALL) $(PROFILE)
AM_CFLAGS = $(WALL) $(PROFILE)
AM_CXXFLAGS = $(WALL) $(PROFILE)
AM_LDFLAGS = $(PROFILE) $(pnetcdf_LDFLAGS) $(GA_LDFLAGS)
LDADD = libpagoda.la $(PNETCDF_LIBS) $(GA_LIBS) $(FLIBS)
LDADD_STANDALONE = $(PNETCDF_LIBS) $(GA_LIBS) $(FLIBS)

check_PROGRAMS = subsetter_old
bin_PROGRAMS = 
lib_LTLIBRARIES = libpagoda.la

subsetter_old_SOURCES = src/Mainp.C

libpagoda_la_SOURCES =        \
src/AbstractVariable.C        \
src/AbstractVariable.H        \
src/Aggregation.C             \
src/AggregationDimension.C    \
src/AggregationDimension.H    \
src/Aggregation.H             \
src/AggregationJoinExisting.C \
src/AggregationJoinExisting.H \
src/AggregationUnion.C        \
src/AggregationUnion.H        \
src/AggregationVariable.C     \
src/AggregationVariable.H     \
src/Array.C                   \
src/Array.H                   \
src/Attribute.C               \
src/Attribute.H               \
src/BoundaryVariable.C        \
src/BoundaryVariable.H        \
src/CommandLineParser.C       \
src/CommandLineParser.H       \
src/Common.H                  \
src/ConnectivityVariable.C    \
src/ConnectivityVariable.H    \
src/CoordinateVariable.C      \
src/CoordinateVariable.H      \
src/Dataset.C                 \
src/Dataset.H                 \
src/DataType.C                \
src/DataType.H                \
src/Debug.C                   \
src/Debug.H                   \
src/Dimension.C               \
src/Dimension.H               \
src/Error.H                   \
src/FileWriter.C              \
src/FileWriter.H              \
src/gax.H                     \
src/gax.C                     \
src/GlobalArray.C             \
src/GlobalArray.H             \
src/GlobalMask.C              \
src/GlobalMask.H              \
src/LatLonBox.C               \
src/LatLonBox.H               \
src/Mask.C                    \
src/Mask.H                    \
src/MaskMap.C                 \
src/MaskMap.H                 \
src/NetcdfAttribute.C         \
src/NetcdfAttribute.H         \
src/NetcdfDataset.C           \
src/NetcdfDataset.H           \
src/NetcdfDimension.C         \
src/NetcdfDimension.H         \
src/NetcdfError.H             \
src/NetcdfFileWriter.C        \
src/NetcdfFileWriter.H        \
src/NetcdfVariable.C          \
src/NetcdfVariable.H          \
src/NotImplementedException.H \
src/pagoda.C                  \
src/pagoda.H                  \
src/Pack.C                    \
src/Pack.H                    \
src/Pnetcdf.C                 \
src/Pnetcdf.H                 \
src/PnetcdfTiming.C           \
src/PnetcdfTiming.H           \
src/RangeException.H          \
src/Slice.H                   \
src/StringComparator.C        \
src/StringComparator.H        \
src/SubsetterCommands.C       \
src/SubsetterCommands.H       \
src/SubsetterException.H      \
src/Timing.C                  \
src/Timing.H                  \
src/TypedValues.H             \
src/Util.C                    \
src/Util.H                    \
src/Values.C                  \
src/Values.H                  \
src/Variable.C                \
src/VariableDecorator.C       \
src/VariableDecorator.H       \
src/Variable.H

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
