ACLOCAL_AMFLAGS = -I m4

if WARNINGS
WALL = -Wall
endif

if PROFILE
PROFILE = -pg
endif

AM_CPPFLAGS = -I$(top_srcdir)/src $(PNETCDF_CPPFLAGS) $(NETCDF4_CPPFLAGS) $(GA_CPPFLAGS)
AM_FFLAGS = $(WALL) $(PROFILE)
AM_CFLAGS = $(WALL) $(PROFILE)
AM_CXXFLAGS = $(WALL) $(PROFILE)
AM_LDFLAGS = $(PROFILE) $(PNETCDF_LDFLAGS) $(NETCDF4_LDFLAGS) $(GA_LDFLAGS)
LDADD = libpagoda.la $(PNETCDF_LIBS) $(NETCDF4_LIBS) $(GA_LIBS) $(FLIBS)
LDADD_STANDALONE = $(PNETCDF_LIBS) $(NETCDF4_LIBS) $(GA_LIBS) $(FLIBS)
EXTRA_DIST =
CLEANFILES =
DISTCLEANFILES =

bin_PROGRAMS = pgdump pgsub pgbo pgra pgea pgcmp
lib_LTLIBRARIES = libpagoda.la

pgdump_SOURCES = src/pgdump.C
pgsub_SOURCES = src/pgsub.C
pgbo_SOURCES = src/pgbo.C
pgra_SOURCES = src/pgra.C
pgea_SOURCES = src/pgea.C
pgcmp_SOURCES = src/pgcmp.C

pkginclude_HEADERS =
pkginclude_HEADERS += src/AbstractArray.H
pkginclude_HEADERS += src/AbstractDataset.H
pkginclude_HEADERS += src/AbstractVariable.H
pkginclude_HEADERS += src/AggregationDimension.H
pkginclude_HEADERS += src/Aggregation.H
pkginclude_HEADERS += src/AggregationJoinExisting.H
pkginclude_HEADERS += src/AggregationUnion.H
pkginclude_HEADERS += src/AggregationVariable.H
pkginclude_HEADERS += src/AllNodeArray.H
pkginclude_HEADERS += src/Array.H
pkginclude_HEADERS += src/Attribute.H
pkginclude_HEADERS += src/Bootstrap.H
pkginclude_HEADERS += src/CommandLineOption.H
pkginclude_HEADERS += src/CommandLineParser.H
pkginclude_HEADERS += src/Common.H
pkginclude_HEADERS += src/CoordHyperslab.H
pkginclude_HEADERS += src/Dataset.H
pkginclude_HEADERS += src/DataType.def
pkginclude_HEADERS += src/DataType.H
pkginclude_HEADERS += src/Debug.H
pkginclude_HEADERS += src/Dimension.H
pkginclude_HEADERS += src/Error.H
pkginclude_HEADERS += src/FileWriter.H
pkginclude_HEADERS += src/gax.H
pkginclude_HEADERS += src/GlobalArray.def
pkginclude_HEADERS += src/GlobalArray.H
pkginclude_HEADERS += src/GlobalMask.H
pkginclude_HEADERS += src/GenericCommands.H
pkginclude_HEADERS += src/GeoGrid.H
pkginclude_HEADERS += src/Grid.H
pkginclude_HEADERS += src/IndexHyperslab.H
pkginclude_HEADERS += src/LatLonBox.H
pkginclude_HEADERS += src/Mask.H
pkginclude_HEADERS += src/MaskMap.H
pkginclude_HEADERS += src/Netcdf4Attribute.H
pkginclude_HEADERS += src/Netcdf4Dataset.H
pkginclude_HEADERS += src/Netcdf4Dimension.H
pkginclude_HEADERS += src/Netcdf4Error.H
pkginclude_HEADERS += src/Netcdf4FileWriter.H
pkginclude_HEADERS += src/Netcdf4.H
pkginclude_HEADERS += src/Netcdf4Timing.H
pkginclude_HEADERS += src/Netcdf4Variable.H
pkginclude_HEADERS += src/NodeZeroArray.H
pkginclude_HEADERS += src/NotImplementedException.H
pkginclude_HEADERS += src/pagoda.H
pkginclude_HEADERS += src/Pack.H
pkginclude_HEADERS += src/PgboCommands.H
pkginclude_HEADERS += src/PgeaCommands.H
pkginclude_HEADERS += src/PgraCommands.H
pkginclude_HEADERS += src/PagodaException.H
pkginclude_HEADERS += src/PnetcdfAttribute.H
pkginclude_HEADERS += src/PnetcdfDataset.H
pkginclude_HEADERS += src/PnetcdfDimension.H
pkginclude_HEADERS += src/PnetcdfError.H
pkginclude_HEADERS += src/PnetcdfFileWriter.H
pkginclude_HEADERS += src/Pnetcdf.H
pkginclude_HEADERS += src/PnetcdfTiming.H
pkginclude_HEADERS += src/PnetcdfVariable.H
pkginclude_HEADERS += src/RangeException.H
pkginclude_HEADERS += src/RegularGrid.H
pkginclude_HEADERS += src/ScalarArray.H
pkginclude_HEADERS += src/Slice.H
pkginclude_HEADERS += src/StringComparator.H
pkginclude_HEADERS += src/SubsetterCommands.H
pkginclude_HEADERS += src/Timing.H
pkginclude_HEADERS += src/TypedValues.H
pkginclude_HEADERS += src/Util.H
pkginclude_HEADERS += src/Values.H
pkginclude_HEADERS += src/Variable.H

libpagoda_la_SOURCES =
libpagoda_la_SOURCES += src/AbstractArray.C
libpagoda_la_SOURCES += src/AbstractDataset.C
libpagoda_la_SOURCES += src/AbstractVariable.C
libpagoda_la_SOURCES += src/Aggregation.C
libpagoda_la_SOURCES += src/AggregationDimension.C
libpagoda_la_SOURCES += src/AggregationJoinExisting.C
libpagoda_la_SOURCES += src/AggregationUnion.C
libpagoda_la_SOURCES += src/AggregationVariable.C
libpagoda_la_SOURCES += src/Array.C
libpagoda_la_SOURCES += src/Attribute.C
libpagoda_la_SOURCES += src/Bootstrap.C
libpagoda_la_SOURCES += src/CommandLineOption.C
libpagoda_la_SOURCES += src/CommandLineParser.C
libpagoda_la_SOURCES += src/CoordHyperslab.C
libpagoda_la_SOURCES += src/Dataset.C
libpagoda_la_SOURCES += src/DataType.C
libpagoda_la_SOURCES += src/Debug.C
libpagoda_la_SOURCES += src/Dimension.C
libpagoda_la_SOURCES += src/FileWriter.C
libpagoda_la_SOURCES += src/gax.C
libpagoda_la_SOURCES += src/GenericAttribute.C
libpagoda_la_SOURCES += src/GlobalArray.C
libpagoda_la_SOURCES += src/GlobalMask.C
libpagoda_la_SOURCES += src/GenericCommands.C
libpagoda_la_SOURCES += src/GeoGrid.C
libpagoda_la_SOURCES += src/Grid.C
libpagoda_la_SOURCES += src/IndexHyperslab.C
libpagoda_la_SOURCES += src/LatLonBox.C
libpagoda_la_SOURCES += src/Mask.C
libpagoda_la_SOURCES += src/MaskMap.C
libpagoda_la_SOURCES += src/Memory.c
libpagoda_la_SOURCES += src/Netcdf4Attribute.C
libpagoda_la_SOURCES += src/Netcdf4.C
libpagoda_la_SOURCES += src/Netcdf4Dataset.C
libpagoda_la_SOURCES += src/Netcdf4Dimension.C
libpagoda_la_SOURCES += src/Netcdf4FileWriter.C
libpagoda_la_SOURCES += src/Netcdf4Timing.C
libpagoda_la_SOURCES += src/Netcdf4Variable.C
libpagoda_la_SOURCES += src/Pack.C
libpagoda_la_SOURCES += src/PgboCommands.C
libpagoda_la_SOURCES += src/PgeaCommands.C
libpagoda_la_SOURCES += src/PgraCommands.C
libpagoda_la_SOURCES += src/PnetcdfAttribute.C
libpagoda_la_SOURCES += src/Pnetcdf.C
libpagoda_la_SOURCES += src/PnetcdfDataset.C
libpagoda_la_SOURCES += src/PnetcdfDimension.C
libpagoda_la_SOURCES += src/PnetcdfFileWriter.C
libpagoda_la_SOURCES += src/PnetcdfTiming.C
libpagoda_la_SOURCES += src/PnetcdfVariable.C
libpagoda_la_SOURCES += src/RegularGrid.C
libpagoda_la_SOURCES += src/ScalarArray.C
libpagoda_la_SOURCES += src/StringComparator.C
libpagoda_la_SOURCES += src/SubsetterCommands.C
libpagoda_la_SOURCES += src/Timing.C
libpagoda_la_SOURCES += src/Util.C
libpagoda_la_SOURCES += src/Values.C
libpagoda_la_SOURCES += src/Variable.C

#############################################################################
# tests
#
check_PROGRAMS =
check_PROGRAMS += tests/FillValue

tests_FillValue_SOURCES = tests/FillValue.C
tests_FillValue_DEPENDENCIES = 
if HAVE_NCGEN
tests_FillValue_DEPENDENCIES += data/FillValue.nc
endif
tests_FillValue_DEPENDENCIES += libpagoda.la

AM_V_NCGEN = $(am__v_NCGEN_$(V))
am__v_NCGEN_ = $(am__v_NCGEN_$(AM_DEFAULT_VERBOSITY))
am__v_NCGEN_0 = @echo "  NCGEN " $@;
.cdl.nc:
	$(AM_V_NCGEN)$(NCGEN) -o $@ $<

EXTRA_DIST += data/FillValue.cdl

data/FillValue.nc: data/$(am__dirstamp)

CLEANFILES += data/FillValue.nc

data/$(am__dirstamp):
	@$(MKDIR_P) data
	@: > data/$(am__dirstamp)

DISTCLEANFILES += data/$(am__dirstamp)

#############################################################################
# sandbox
#
if ENABLE_SANDBOX
if ENABLE_SANDBOX_GA
check_PROGRAMS += sandbox/ga/TestAttribute
check_PROGRAMS += sandbox/ga/TestBin
check_PROGRAMS += sandbox/ga/TestDimension
check_PROGRAMS += sandbox/ga/TestDistance
check_PROGRAMS += sandbox/ga/TestEnum
check_PROGRAMS += sandbox/ga/TestGet
check_PROGRAMS += sandbox/ga/TestJoin
check_PROGRAMS += sandbox/ga/TestLocateRegion
check_PROGRAMS += sandbox/ga/TestPnetcdfDataset
check_PROGRAMS += sandbox/ga/TestPartialSum
check_PROGRAMS += sandbox/ga/TestRavel
check_PROGRAMS += sandbox/ga/TestRead
check_PROGRAMS += sandbox/ga/TestScanAdd
check_PROGRAMS += sandbox/ga/TestSort
check_PROGRAMS += sandbox/ga/TestUnion
check_PROGRAMS += sandbox/ga/TestUnpack
endif # ENABLE_SANDBOX_GA
check_PROGRAMS += sandbox/TestArray
check_PROGRAMS += sandbox/TestBin
check_PROGRAMS += sandbox/TestCommandLine
check_PROGRAMS += sandbox/TestDimension
check_PROGRAMS += sandbox/TestDistance
check_PROGRAMS += sandbox/TestEnum
check_PROGRAMS += sandbox/TestGet
check_PROGRAMS += sandbox/TestGrid
check_PROGRAMS += sandbox/TestJoin
check_PROGRAMS += sandbox/TestPnetcdfAttribute
check_PROGRAMS += sandbox/TestPnetcdfDataset
check_PROGRAMS += sandbox/TestMaskMap
check_PROGRAMS += sandbox/TestPartialSum
check_PROGRAMS += sandbox/TestRavel
check_PROGRAMS += sandbox/TestRead
check_PROGRAMS += sandbox/TestRead2
check_PROGRAMS += sandbox/TestScatter
check_PROGRAMS += sandbox/TestSlice
check_PROGRAMS += sandbox/TestUnion
check_PROGRAMS += sandbox/TestWrite

sandbox_ga_TestAttribute_SOURCES      = sandbox/ga/TestAttribute.C
sandbox_ga_TestBin_SOURCES            = sandbox/ga/TestBin.C
sandbox_ga_TestDimension_SOURCES      = sandbox/ga/TestDimension.C
sandbox_ga_TestDistance_SOURCES       = sandbox/ga/TestDistance.C
sandbox_ga_TestEnum_SOURCES           = sandbox/ga/TestEnum.C
sandbox_ga_TestGet_SOURCES            = sandbox/ga/TestGet.C
sandbox_ga_TestJoin_SOURCES           = sandbox/ga/TestJoin.C
sandbox_ga_TestLocateRegion_SOURCES   = sandbox/ga/TestLocateRegion.C
sandbox_ga_TestPnetcdfDataset_SOURCES = sandbox/ga/TestPnetcdfDataset.C
sandbox_ga_TestPartialSum_SOURCES     = sandbox/ga/TestPartialSum.C
sandbox_ga_TestRavel_SOURCES          = sandbox/ga/TestRavel.C
sandbox_ga_TestRead_SOURCES           = sandbox/ga/TestRead.C
sandbox_ga_TestScanAdd_SOURCES        = sandbox/ga/TestScanAdd.C
sandbox_ga_TestSort_SOURCES           = sandbox/ga/TestSort.C
sandbox_ga_TestUnion_SOURCES          = sandbox/ga/TestUnion.C
sandbox_ga_TestUnpack_SOURCES         = sandbox/ga/TestUnpack.c
sandbox_TestArray_SOURCES             = sandbox/TestArray.C
sandbox_TestBin_SOURCES               = sandbox/TestBin.C
sandbox_TestCommandLine_SOURCES       = sandbox/TestCommandLine.C
sandbox_TestDimension_SOURCES         = sandbox/TestDimension.C
sandbox_TestDistance_SOURCES          = sandbox/TestDistance.C
sandbox_TestEnum_SOURCES              = sandbox/TestEnum.C
sandbox_TestGet_SOURCES               = sandbox/TestGet.C
sandbox_TestGrid_SOURCES              = sandbox/TestGrid.C
sandbox_TestJoin_SOURCES              = sandbox/TestJoin.C
sandbox_TestPnetcdfAttribute_SOURCES  = sandbox/TestPnetcdfAttribute.C
sandbox_TestPnetcdfDataset_SOURCES    = sandbox/TestPnetcdfDataset.C
sandbox_TestMaskMap_SOURCES           = sandbox/TestMaskMap.C
sandbox_TestPartialSum_SOURCES        = sandbox/TestPartialSum.C
sandbox_TestRavel_SOURCES             = sandbox/TestRavel.C
sandbox_TestRead_SOURCES              = sandbox/TestRead.C
sandbox_TestRead2_SOURCES             = sandbox/TestRead2.C
sandbox_TestScatter_SOURCES           = sandbox/TestScatter.C
sandbox_TestSlice_SOURCES             = sandbox/TestSlice.C
sandbox_TestUnion_SOURCES             = sandbox/TestUnion.C
sandbox_TestWrite_SOURCES             = sandbox/TestWrite.C

sandbox_ga_TestGet_LDADD = $(LDADD_STANDALONE)
endif # ENABLE_SANDBOX
