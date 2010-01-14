check_PROGRAMS = \
  Test \
  TestAttribute \
  TestDimension \
  TestEnum \
  TestGet \
  TestLocateRegion \
  TestJoin \
  TestNetcdfDataset \
  TestOpen \
  TestPartialSum \
  TestRavel \
  TestRead \
  TestScanAdd \
  TestUnion \
  TestUnpack

AM_CPPFLAGS = $(GA_INCS) -I$(top_srcdir)
AM_LDFLAGS = $(GA_LIBS) $(FLIBS)
LDADD = ../libsubsetter.la

Test_SOURCES = Test.C
TestAttribute_SOURCES = TestAttribute.C
TestDimension_SOURCES = TestDimension.C
TestEnum_SOURCES = TestEnum.C
TestGet_SOURCES = TestGet.C
TestJoin_SOURCES = TestJoin.C
TestLocateRegion_SOURCES = TestLocateRegion.C
TestNetcdfDataset_SOURCES = TestNetcdfDataset.C
TestOpen_SOURCES = TestOpen.C
TestPartialSum_SOURCES = TestPartialSum.C
TestRavel_SOURCES = TestRavel.C
TestRead_SOURCES = TestRead.C
TestScanAdd_SOURCES = TestScanAdd.C
TestUnion_SOURCES = TestUnion.C
TestUnpack_SOURCES = TestUnpack.c
