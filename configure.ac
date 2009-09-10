# Initialize autoconf.
AC_PREREQ([2.63])
AC_INIT([GCRM PARALLEL SUBSETTER], [0.1], [jeff.daily@pnl.gov])
AC_CONFIG_SRCDIR([Mainp.C])
AC_CONFIG_HEADERS([config/config.h])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([config])

# Checks for compilers and compiler characteristics.
AC_PROG_CC([mpicc hcc mpxlc_r mpxlc mpcc cmpicc cc])
AC_PROG_CXX([mpic++ mpicxx mpiCC hcp mpxlC_r mpxlC mpCC cmpic++ CC])
AC_PROG_F77([mpif77 hf77 mpxlf_r mpxlf mpf77 cmpifc ftn])
#AC_PROG_FC([mpif90 mpxlf95_r mpxlf90_r mpxlf95 mpxlf90 mpf90 cmpif90c ftn])
#AC_FC_LIBRARY_LDFLAGS
#AC_FC_DUMMY_MAIN
AC_F77_LIBRARY_LDFLAGS
AC_F77_DUMMY_MAIN

# Initialize automake and libtool.
AM_INIT_AUTOMAKE
AM_DISABLE_SHARED
LT_INIT

# Checks for libraries.
GCRM_CHECK_PACKAGE([pnetcdf], [ncmpi_open], [pnetcdf], [pnetcdf.h],
  [AC_SUBST([LIBPNETCDF], [-lpnetcdf])
   AC_DEFINE([HAVE_LIBPNETCDF], [1], [Define if you have libpnetcdf])
  ],
  [AC_MSG_FAILURE([--with-pnetcdf was given, but test for pnetcdf failed])]
)

# Where to find Global Arrays.
AC_ARG_VAR([GA_INCS], [Global Arrays include directives])
AC_SUBST([GA_INCS])
AC_ARG_VAR([GA_LIBS], [Global Arrays C linker directives])
AC_SUBST([GA_LIBS])

# Debug helper stuff.
AC_ARG_ENABLE([debug],
    [AS_HELP_STRING([--enable-debug], [stderr: unordered debug messages])],
    [AC_DEFINE_UNQUOTED([DEBUG], [1], [stderr: unordered debug messages])])
AC_ARG_ENABLE([trace],
    [AS_HELP_STRING([--enable-trace], [stderr: ordered function calls])],
    [AC_DEFINE_UNQUOTED([TRACE], [1], [stderr: ordered function calls])])
AC_ARG_ENABLE([profile],
    [AS_HELP_STRING([--enable-profile], [enable gprof symbols])])
AM_CONDITIONAL([PROFILE], [test x$enable_profile = xyes])
AC_ARG_ENABLE([warnings],
    [AS_HELP_STRING([--enable-warnings], [enable -Wall warnings])])
AM_CONDITIONAL([WARNINGS], [test x$enable_warnings = xyes])

# Checks for header files.
AC_CHECK_HEADERS([limits.h stdlib.h unistd.h])

#
# Check for some types
#

AC_CHECK_TYPE([MPI_Offset], [], [], [[#include <mpi.h>]])
AC_CHECK_TYPE([int64_t])

#
# Check for type sizes
#

AC_CHECK_SIZEOF([MPI_Offset], [], [[#include <mpi.h>]])
AC_CHECK_SIZEOF([int64_t])

# Checks for typedefs, structures, and compiler characteristics.
AM_PROG_CC_C_O
AC_HEADER_STDBOOL

# Checks for library functions.
AC_CHECK_FUNCS([strtol])

# NERSC's MPI library requires all C++ codes to include mpi.h before any
# other include files.  I'm not sure if this includes config.h as well, but
# putting this here will at least put mpi.h before any OTHER include files.
AH_BOTTOM([#include <mpi.h>])

AC_CONFIG_FILES([Makefile
                 tests/Makefile])
AC_OUTPUT
