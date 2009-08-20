check_PROGRAMS = \
  Test \
  TestAttribute \
  TestDimension \
  TestEnum \
  TestGather \
  TestGet \
  TestLocateRegion \
  TestNetcdfDataset \
  TestOpen \
  TestPartialSum \
  TestRavel \
  TestScanAdd \
  TestUnion \
  TestUnpack

AM_CFLAGS = $(GA_INCS) -I$(top_srcdir)
AM_CXXFLAGS = $(GA_INCS) -I$(top_srcdir) $(CFLAGS) 
#AM_LDFLAGS = $(GA_LIBS) $(FCLIBS)
AM_LDFLAGS = $(GA_LIBS) $(FLIBS)

Test_SOURCES = Test.C
Test_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
Test_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)

TestAttribute_SOURCES = TestAttribute.C
TestAttribute_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestAttribute_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestAttribute_LDADD = ../libsubsetter.la

TestDimension_SOURCES = TestDimension.C
TestDimension_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestDimension_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestDimension_LDADD = ../libsubsetter.la

TestEnum_SOURCES = TestEnum.C
TestEnum_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestEnum_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestEnum_LDADD = ../libsubsetter.la

TestGather_SOURCES = TestGather.c
TestGather_CFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CFLAGS)
TestGather_LDFLAGS = -L/home/d3n000/gcrm/local/lib $(AM_LDFLAGS)

TestGet_SOURCES = TestGet.C
TestGet_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestGet_LDFLAGS = -L/home/d3n000/gcrm/local/lib $(AM_LDFLAGS)
TestGet_LDADD = ../libsubsetter.la

TestLocateRegion_SOURCES = TestLocateRegion.C
TestLocateRegion_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestLocateRegion_LDFLAGS = -L/home/d3n000/gcrm/local/lib $(AM_LDFLAGS)
TestLocateRegion_LDADD = ../libsubsetter.la

TestNetcdfDataset_SOURCES = TestNetcdfDataset.C
TestNetcdfDataset_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestNetcdfDataset_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestNetcdfDataset_LDADD = ../libsubsetter.la

TestOpen_SOURCES = TestOpen.C
TestOpen_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestOpen_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)

TestPartialSum_SOURCES = TestPartialSum.C
TestPartialSum_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestPartialSum_LDFLAGS = -L/home/d3n000/gcrm/local/lib $(AM_LDFLAGS)
TestPartialSum_LDADD = ../libsubsetter.la

TestRavel_SOURCES = TestRavel.C
TestRavel_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestRavel_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestRavel_LDADD = ../libsubsetter.la

TestScanAdd_SOURCES = TestScanAdd.C
TestScanAdd_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestScanAdd_LDFLAGS = -L/home/d3n000/gcrm/local/lib $(AM_LDFLAGS)
TestScanAdd_LDADD = ../libsubsetter.la

TestUnion_SOURCES = TestUnion.C
TestUnion_CXXFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CXXFLAGS)
TestUnion_LDFLAGS = -L/home/d3n000/gcrm/local/lib -lnetcdf $(AM_LDFLAGS)
TestUnion_LDADD = ../libsubsetter.la

TestUnpack_SOURCES = TestUnpack.c
TestUnpack_CFLAGS = -I/home/d3n000/gcrm/local/include $(AM_CFLAGS)
TestUnpack_LDFLAGS = -L/home/d3n000/gcrm/local/lib $(AM_LDFLAGS)

