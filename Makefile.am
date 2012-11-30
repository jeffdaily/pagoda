ACLOCAL_AMFLAGS = -I m4

if WARNINGS
WALL = -Wall
endif

if PROFILE
PROFILE = -pg
endif

AM_CPPFLAGS =
AM_CPPFLAGS += -I$(top_srcdir)/src
AM_CPPFLAGS += $(BIL_CPPFLAGS)
AM_CPPFLAGS += $(PNETCDF_CPPFLAGS)
AM_CPPFLAGS += $(NETCDF4_CPPFLAGS)
AM_CPPFLAGS += $(GA_CPPFLAGS)

AM_FFLAGS = $(WALL) $(PROFILE)
AM_CFLAGS = $(WALL) $(PROFILE)
AM_CXXFLAGS = $(WALL) $(PROFILE)

AM_LDFLAGS =
AM_LDFLAGS += $(PROFILE)
AM_LDFLAGS += $(BIL_LDFLAGS)
AM_LDFLAGS += $(PNETCDF_LDFLAGS)
AM_LDFLAGS += $(NETCDF4_LDFLAGS)
AM_LDFLAGS += $(GA_LDFLAGS)

LDADD =
LDADD += libpagoda.la
LDADD += $(BIL_LIBS)
LDADD += $(PNETCDF_LIBS)
LDADD += $(NETCDF4_LIBS)
LDADD += $(GA_LIBS)

LDADD_STANDALONE =
LDADD_STANDALONE += $(BIL_LIBS)
LDADD_STANDALONE += $(PNETCDF_LIBS)
LDADD_STANDALONE += $(NETCDF4_LIBS)
LDADD_STANDALONE += $(GA_LIBS)

if GA_USES_FLIBS
LDADD += $(FLIBS)
LDADD_STANDALONE += $(FLIBS)
endif

EXTRA_DIST =
MOSTLYCLEANFILES =
CLEANFILES =
DISTCLEANFILES =
MAINTAINERCLEANFILES =

bin_PROGRAMS =
bin_PROGRAMS += pgbo
bin_PROGRAMS += pgcmp
bin_PROGRAMS += pgdump
bin_PROGRAMS += pgea
bin_PROGRAMS += pgecat
bin_PROGRAMS += pgflint
bin_PROGRAMS += pgpdq
bin_PROGRAMS += pgra
bin_PROGRAMS += pgrcat
bin_PROGRAMS += pgrsub
bin_PROGRAMS += pgsub
bin_PROGRAMS += pgwa
bin_PROGRAMS += pgwa_basic
lib_LTLIBRARIES = libpagoda.la
check_PROGRAMS =
check_DATA =
pkginclude_HEADERS =
pagodadir = $(datadir)/pagoda

pgbo_SOURCES       = src/pgbo.C
pgcmp_SOURCES      = src/pgcmp.C
pgdump_SOURCES     = src/pgdump.C
pgea_SOURCES       = src/pgea.C
pgecat_SOURCES     = src/pgecat.C
pgflint_SOURCES    = src/pgflint.C
pgpdq_SOURCES      = src/pgpdq.C
pgra_SOURCES       = src/pgra.C
pgrcat_SOURCES     = src/pgrcat.C
pgrsub_SOURCES     = src/pgrsub.C
pgsub_SOURCES      = src/pgsub.C
pgwa_SOURCES       = src/pgwa.C
pgwa_basic_SOURCES = src/pgwa_basic.C

pkginclude_HEADERS += src/AbstractArray.H
pkginclude_HEADERS += src/AbstractDataset.H
pkginclude_HEADERS += src/AbstractFileWriter.H
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
pkginclude_HEADERS += src/Collectives.H
pkginclude_HEADERS += src/CommandLineOption.H
pkginclude_HEADERS += src/CommandLineParser.H
pkginclude_HEADERS += src/Common.H
pkginclude_HEADERS += src/Copy.H
pkginclude_HEADERS += src/CoordHyperslab.H
pkginclude_HEADERS += src/Dataset.H
pkginclude_HEADERS += src/DataType.def
pkginclude_HEADERS += src/DataType2.def
pkginclude_HEADERS += src/DataType.H
pkginclude_HEADERS += src/Debug.H
pkginclude_HEADERS += src/Dimension.H
pkginclude_HEADERS += src/Error.H
pkginclude_HEADERS += src/FileFormat.H
pkginclude_HEADERS += src/FileWriter.H
pkginclude_HEADERS += src/GenericCommands.H
pkginclude_HEADERS += src/GeoGrid.H
pkginclude_HEADERS += src/Grid.H
pkginclude_HEADERS += src/Hints.H
pkginclude_HEADERS += src/IndexHyperslab.H
pkginclude_HEADERS += src/LatLonBox.H
pkginclude_HEADERS += src/MaskMap.H
pkginclude_HEADERS += src/NodeZeroArray.H
pkginclude_HEADERS += src/NotImplementedException.H
pkginclude_HEADERS += src/Numeric.H
pkginclude_HEADERS += src/pagoda.H
pkginclude_HEADERS += src/Pack.H
pkginclude_HEADERS += src/PagodaException.H
pkginclude_HEADERS += src/PgboCommands.H
pkginclude_HEADERS += src/PgeaCommands.H
pkginclude_HEADERS += src/PgecatCommands.H
pkginclude_HEADERS += src/PgflintCommands.H
pkginclude_HEADERS += src/PgpdqCommands.H
pkginclude_HEADERS += src/PgraCommands.H
pkginclude_HEADERS += src/PgrcatCommands.H
pkginclude_HEADERS += src/PgrsubCommands.H
pkginclude_HEADERS += src/PgwaCommands.H
pkginclude_HEADERS += src/Print.H
pkginclude_HEADERS += src/ProcessGroup.H
pkginclude_HEADERS += src/RangeException.H
pkginclude_HEADERS += src/RegularGrid.H
pkginclude_HEADERS += src/ScalarArray.H
pkginclude_HEADERS += src/Slice.H
pkginclude_HEADERS += src/StringComparator.H
pkginclude_HEADERS += src/SubsetterCommands.H
#pkginclude_HEADERS += src/Timing.H
pkginclude_HEADERS += src/TypedValues.H
pkginclude_HEADERS += src/Util.H
pkginclude_HEADERS += src/Validator.H
pkginclude_HEADERS += src/ValidMax.H
pkginclude_HEADERS += src/ValidMin.H
pkginclude_HEADERS += src/ValidRange.H
pkginclude_HEADERS += src/Values.H
pkginclude_HEADERS += src/Variable.H

libpagoda_la_SOURCES =
libpagoda_la_SOURCES += src/AbstractArray.C
libpagoda_la_SOURCES += src/AbstractArray_op.C
libpagoda_la_SOURCES += src/AbstractArray_op_v.C
libpagoda_la_SOURCES += src/AbstractArray_op_vc.C
libpagoda_la_SOURCES += src/AbstractArray_op_scalar.C
libpagoda_la_SOURCES += src/AbstractArray_op_scalar_v.C
libpagoda_la_SOURCES += src/AbstractArray_op_scalar_vc.C
libpagoda_la_SOURCES += src/AbstractDataset.C
libpagoda_la_SOURCES += src/AbstractFileWriter.C
libpagoda_la_SOURCES += src/AbstractVariable.C
libpagoda_la_SOURCES += src/Aggregation.C
libpagoda_la_SOURCES += src/AggregationDimension.C
libpagoda_la_SOURCES += src/AggregationJoinExisting.C
libpagoda_la_SOURCES += src/AggregationUnion.C
libpagoda_la_SOURCES += src/AggregationVariable.C
libpagoda_la_SOURCES += src/Array.C
libpagoda_la_SOURCES += src/Attribute.C
libpagoda_la_SOURCES += src/Bootstrap.C
libpagoda_la_SOURCES += src/Collectives.C
libpagoda_la_SOURCES += src/CommandLineOption.C
libpagoda_la_SOURCES += src/CommandLineParser.C
libpagoda_la_SOURCES += src/Copy.C
libpagoda_la_SOURCES += src/CoordHyperslab.C
libpagoda_la_SOURCES += src/Dataset.C
libpagoda_la_SOURCES += src/DataType.C
libpagoda_la_SOURCES += src/Dimension.C
libpagoda_la_SOURCES += src/FileFormat.C
libpagoda_la_SOURCES += src/FileWriter.C
libpagoda_la_SOURCES += src/GenericAttribute.C
libpagoda_la_SOURCES += src/GenericCommands.C
libpagoda_la_SOURCES += src/GeoGrid.C
libpagoda_la_SOURCES += src/Grid.C
libpagoda_la_SOURCES += src/Hints.C
libpagoda_la_SOURCES += src/IndexHyperslab.C
libpagoda_la_SOURCES += src/LatLonBox.C
libpagoda_la_SOURCES += src/MaskMap.C
libpagoda_la_SOURCES += src/Numeric.C
libpagoda_la_SOURCES += src/Pack.C
libpagoda_la_SOURCES += src/PgboCommands.C
libpagoda_la_SOURCES += src/PgeaCommands.C
libpagoda_la_SOURCES += src/PgecatCommands.C
libpagoda_la_SOURCES += src/PgflintCommands.C
libpagoda_la_SOURCES += src/PgpdqCommands.C
libpagoda_la_SOURCES += src/PgraCommands.C
libpagoda_la_SOURCES += src/PgrcatCommands.C
libpagoda_la_SOURCES += src/PgrsubCommands.C
libpagoda_la_SOURCES += src/PgwaCommands.C
libpagoda_la_SOURCES += src/Print.C
libpagoda_la_SOURCES += src/ProcessGroup.C
libpagoda_la_SOURCES += src/RegularGrid.C
libpagoda_la_SOURCES += src/ScalarArray.C
libpagoda_la_SOURCES += src/StringComparator.C
libpagoda_la_SOURCES += src/SubsetterCommands.C
#libpagoda_la_SOURCES += src/Timing.C
libpagoda_la_SOURCES += src/Util.C
libpagoda_la_SOURCES += src/Validator.C
libpagoda_la_SOURCES += src/Values.C
libpagoda_la_SOURCES += src/Variable.C

if HAVE_GA
#libpagoda_la_SOURCES += src/gax.C
#libpagoda_la_SOURCES += src/gax.H
libpagoda_la_SOURCES += src/GlobalArray.C
libpagoda_la_SOURCES += src/GlobalArray.H
libpagoda_la_SOURCES += src/GlobalArray.def
libpagoda_la_SOURCES += src/GlobalScalar.C
libpagoda_la_SOURCES += src/GlobalScalar.H
libpagoda_la_SOURCES += src/Memory.c
endif

if HAVE_PNETCDF
libpagoda_la_SOURCES += src/PnetcdfAttribute.C
libpagoda_la_SOURCES += src/PnetcdfAttribute.H
libpagoda_la_SOURCES += src/PnetcdfDataset.C
libpagoda_la_SOURCES += src/PnetcdfDataset.H
libpagoda_la_SOURCES += src/PnetcdfDimension.C
libpagoda_la_SOURCES += src/PnetcdfDimension.H
libpagoda_la_SOURCES += src/PnetcdfError.H
libpagoda_la_SOURCES += src/PnetcdfFileWriter.C
libpagoda_la_SOURCES += src/PnetcdfFileWriter.H
libpagoda_la_SOURCES += src/PnetcdfNS.C
libpagoda_la_SOURCES += src/PnetcdfNS.H
libpagoda_la_SOURCES += src/PnetcdfTiming.C
libpagoda_la_SOURCES += src/PnetcdfTiming.H
libpagoda_la_SOURCES += src/PnetcdfVariable.C
libpagoda_la_SOURCES += src/PnetcdfVariable.H
endif

if HAVE_NETCDF4
libpagoda_la_SOURCES += src/Netcdf4.C
libpagoda_la_SOURCES += src/Netcdf4.H
libpagoda_la_SOURCES += src/Netcdf4Attribute.C
libpagoda_la_SOURCES += src/Netcdf4Attribute.H
libpagoda_la_SOURCES += src/Netcdf4Dataset.C
libpagoda_la_SOURCES += src/Netcdf4Dataset.H
libpagoda_la_SOURCES += src/Netcdf4Dimension.C
libpagoda_la_SOURCES += src/Netcdf4Dimension.H
libpagoda_la_SOURCES += src/Netcdf4Error.H
libpagoda_la_SOURCES += src/Netcdf4FileWriter.C
libpagoda_la_SOURCES += src/Netcdf4FileWriter.H
libpagoda_la_SOURCES += src/Netcdf4Timing.C
libpagoda_la_SOURCES += src/Netcdf4Timing.H
libpagoda_la_SOURCES += src/Netcdf4Variable.C
libpagoda_la_SOURCES += src/Netcdf4Variable.H
endif

#############################################################################
# tests (autotest suite)
#
check_PROGRAMS += tests/TestArrayGetMask
check_PROGRAMS += tests/TestPartialSum

tests_TestArrayGetMask_SOURCES = tests/TestArrayGetMask.C
tests_TestPartialSum_SOURCES   = tests/TestPartialSum.C

TEST_DIR     = tests
TESTSUITE_AT = $(TESTSUITE).at
TESTSUITE    = $(TEST_DIR)/suite
PACKAGE_M4   = $(TEST_DIR)/package.m4
ATLOCAL_IN   = $(TEST_DIR)/atlocal.in
ATCONFIG     = $(TEST_DIR)/atconfig
ATLOCAL      = $(TEST_DIR)/atlocal
LOCAL_AT     = $(TEST_DIR)/local.at
PGRA_AT      = $(TEST_DIR)/pgra.at
AUTOM4TE     = $(SHELL) $(srcdir)/build-aux/missing --run autom4te
AUTOTEST     = $(AUTOM4TE) --language=autotest
EXTRA_DIST  += $(TESTSUITE_AT)
EXTRA_DIST  += $(PACKAGE_M4)
EXTRA_DIST  += $(TESTSUITE)
EXTRA_DIST  += $(ATLOCAL_IN)
EXTRA_DIST  += $(LOCAL_AT)
EXTRA_DIST  += $(PGRA_AT)

recheck: $(ATCONFIG) $(ATLOCAL) $(TESTSUITE)
	$(SHELL) '$(TESTSUITE)' AUTOTEST_PATH='.' -C $(TEST_DIR) $(TESTSUITEFLAGS) --recheck

check-local: $(ATCONFIG) $(ATLOCAL) $(TESTSUITE)
	$(SHELL) '$(TESTSUITE)' AUTOTEST_PATH='.' -C $(TEST_DIR) $(TESTSUITEFLAGS)

installcheck-local: $(ATCONFIG) $(ATLOCAL) $(TESTSUITE)
	$(SHELL) '$(TESTSUITE)' AUTOTEST_PATH='$(bindir)' $(TESTSUITEFLAGS)

clean-local:
	test ! -f '$(TESTSUITE)' || $(SHELL) '$(TESTSUITE)' --clean

# The `:;' works around a Bash 3.2 bug when the output is not writeable.
$(PACKAGE_M4): $(top_srcdir)/configure.ac
	:;{ \
	  echo '# Signature of the current package.' && \
	  echo 'm4_define([AT_PACKAGE_NAME],' && \
	  echo '  [$(PACKAGE_NAME)])' && \
	  echo 'm4_define([AT_PACKAGE_TARNAME],' && \
	  echo '  [$(PACKAGE_TARNAME)])' && \
	  echo 'm4_define([AT_PACKAGE_VERSION],' && \
	  echo '  [$(PACKAGE_VERSION)])' && \
	  echo 'm4_define([AT_PACKAGE_STRING],' && \
	  echo '  [$(PACKAGE_STRING)])' && \
	  echo 'm4_define([AT_PACKAGE_BUGREPORT],' && \
	  echo '  [$(PACKAGE_BUGREPORT)])'; \
	  echo 'm4_define([AT_PACKAGE_URL],' && \
	  echo '  [$(PACKAGE_URL)])'; \
	} >'$(PACKAGE_M4)'

$(TESTSUITE): $(TESTSUITE_AT) $(PACKAGE_M4) $(LOCAL_AT) $(PGRA_AT)
	$(AUTOTEST) -I . -I ./tests -I '$(srcdir)' -I '$(srcdir)/tests' $@.at -o $@.tmp
	mv $@.tmp $@

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

sandbox_ga_TestGet_LDADD = $(LDADD_STANDALONE)
endif # ENABLE_SANDBOX_GA

check_PROGRAMS += sandbox/TestArray
check_PROGRAMS += sandbox/TestCommandLine
check_PROGRAMS += sandbox/TestDimension
check_PROGRAMS += sandbox/TestEnum
check_PROGRAMS += sandbox/TestGet
check_PROGRAMS += sandbox/TestGrid
check_PROGRAMS += sandbox/TestGroups
check_PROGRAMS += sandbox/TestMaskMap
check_PROGRAMS += sandbox/TestNumeric
check_PROGRAMS += sandbox/TestRavel
check_PROGRAMS += sandbox/TestRead
check_PROGRAMS += sandbox/TestReduction
check_PROGRAMS += sandbox/TestScatter
check_PROGRAMS += sandbox/TestSlice
check_PROGRAMS += sandbox/TestTemplates
check_PROGRAMS += sandbox/TestTranspose
check_PROGRAMS += sandbox/TestTranspose2
check_PROGRAMS += sandbox/TestTranspose3
check_PROGRAMS += sandbox/TestUnion
check_PROGRAMS += sandbox/TestVecBool
check_PROGRAMS += sandbox/TestWrite
if HAVE_PNETCDF
check_PROGRAMS += sandbox/TestBin
check_PROGRAMS += sandbox/TestDistance
check_PROGRAMS += sandbox/TestJoin
check_PROGRAMS += sandbox/TestPnetcdfAttribute
check_PROGRAMS += sandbox/TestPnetcdfDataset
check_PROGRAMS += sandbox/TestRead2
check_PROGRAMS += sandbox/TestRead2b
check_PROGRAMS += sandbox/TestRead2c
check_PROGRAMS += sandbox/TestRead2d
endif
if HAVE_NETCDF4
check_PROGRAMS += sandbox/TestRead3
endif

sandbox_TestArray_SOURCES             = sandbox/TestArray.C
sandbox_TestBin_SOURCES               = sandbox/TestBin.C
sandbox_TestCommandLine_SOURCES       = sandbox/TestCommandLine.C
sandbox_TestDimension_SOURCES         = sandbox/TestDimension.C
sandbox_TestDistance_SOURCES          = sandbox/TestDistance.C
sandbox_TestEnum_SOURCES              = sandbox/TestEnum.C
sandbox_TestGet_SOURCES               = sandbox/TestGet.C
sandbox_TestGrid_SOURCES              = sandbox/TestGrid.C
sandbox_TestGroups_SOURCES            = sandbox/TestGroups.C
sandbox_TestJoin_SOURCES              = sandbox/TestJoin.C
sandbox_TestPnetcdfAttribute_SOURCES  = sandbox/TestPnetcdfAttribute.C
sandbox_TestPnetcdfDataset_SOURCES    = sandbox/TestPnetcdfDataset.C
sandbox_TestMaskMap_SOURCES           = sandbox/TestMaskMap.C
sandbox_TestNumeric_SOURCES           = sandbox/TestNumeric.C
sandbox_TestRavel_SOURCES             = sandbox/TestRavel.C
sandbox_TestRead_SOURCES              = sandbox/TestRead.C
sandbox_TestRead2_SOURCES             = sandbox/TestRead2.C
sandbox_TestRead2b_SOURCES            = sandbox/TestRead2b.C
sandbox_TestRead2c_SOURCES            = sandbox/TestRead2c.C
sandbox_TestRead2d_SOURCES            = sandbox/TestRead2d.C
sandbox_TestRead3_SOURCES             = sandbox/TestRead3.C
sandbox_TestReduction_SOURCES         = sandbox/TestReduction.C
sandbox_TestScatter_SOURCES           = sandbox/TestScatter.C
sandbox_TestSlice_SOURCES             = sandbox/TestSlice.C
sandbox_TestTemplates_SOURCES         = sandbox/TestTemplates.C
sandbox_TestTranspose_SOURCES         = sandbox/TestTranspose.C
sandbox_TestTranspose2_SOURCES        = sandbox/TestTranspose2.C
sandbox_TestTranspose3_SOURCES        = sandbox/TestTranspose3.C
sandbox_TestUnion_SOURCES             = sandbox/TestUnion.C
sandbox_TestVecBool_SOURCES           = sandbox/TestVecBool.C
sandbox_TestWrite_SOURCES             = sandbox/TestWrite.C

sandbox_TestTemplates_LDADD = $(LDADD_STANDALONE)

checkprogs: $(check_PROGRAMS)

endif # ENABLE_SANDBOX
