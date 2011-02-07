#####################################################3
# autoconf
#####################################################3
AC_PREREQ([2.64])
AC_INIT([pagoda], [0.6], [pagoda-dev@googlegroups.com], [pagoda],
        [https://svn.pnl.gov/gcrm])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/pagoda.H])
AC_CONFIG_TESTDIR([tests])

#####################################################3
# automake
#####################################################3
AM_INIT_AUTOMAKE([foreign parallel-tests silent-rules subdir-objects])
AM_DISABLE_SHARED

#####################################################3
# C
#####################################################3
AC_MSG_NOTICE
AC_MSG_NOTICE([C tests])
AC_MSG_NOTICE
AC_LANG_PUSH([C])

# Checks for C programs.
AC_PROG_CC([mpicc hcc mpxlc_r mpxlc mpcc cmpicc cc])
PAGODA_MPI_UNWRAP

# Needed so that _GNU_SOURCE is defined thus enabling getopt_long.
AC_USE_SYSTEM_EXTENSIONS

# Checks for C header files.
AC_CHECK_HEADERS([inttypes.h stdint.h stdlib.h sys/time.h time.h unistd.h])

# Checks for C typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE

# Checks for C type sizes.

# Checks for C library functions.

# Checks for C libraries.

AC_LANG_POP([C])

#####################################################3
# Fortran 77
#####################################################3
AC_MSG_NOTICE
AC_MSG_NOTICE([Fortran 77 tests])
AC_MSG_NOTICE
AC_LANG_PUSH([Fortran 77])

# Checks for Fortran 77 programs.
AC_PROG_F77([mpif77 hf77 mpxlf_r mpxlf mpf77 cmpifc ftn])
PAGODA_MPI_UNWRAP

# Checks for Fortran 77 header files.

# Checks for Fortran 77 typedefs, structures, and compiler characteristics.
PAGODA_MPI_UNWRAP_PUSH
AC_F77_LIBRARY_LDFLAGS
PAGODA_MPI_UNWRAP_POP
AC_F77_WRAPPERS

# Checks for Fortran 77 type sizes.

# Checks for Fortran 77 library functions.

# Checks for Fortran 77 libraries.

AC_LANG_POP([Fortran 77])

#####################################################3
# C++
#####################################################3
AC_MSG_NOTICE
AC_MSG_NOTICE([C++ tests])
AC_MSG_NOTICE
AC_LANG_PUSH([C++])

# Checks for C++ programs.
AC_PROG_CXX([mpic++ mpicxx mpiCC hcp mpxlC_r mpxlC mpCC cmpic++ CC])
PAGODA_MPI_UNWRAP

# Checks for C++ header files.

# Checks for C++ typedefs, structures, and compiler characteristics.
#PAGODA_CHECK_FLIBS
AC_F77_DUMMY_MAIN
AC_TYPE_LONG_LONG_INT
AC_TYPE_UNSIGNED_LONG_LONG_INT
AC_TYPE_INT64_T
AC_TYPE_UINT64_T
AC_CHECK_TYPE([MPI_Offset], [], [], [[#include <mpi.h>]])
AC_TYPE_LONG_DOUBLE
PAGODA_CXX_VARIADIC_MACROS([],
    [AC_MSG_WARN([C99 variadic macros not supported, IO timing disabled])])

# Checks for C++ type sizes.
AC_CHECK_SIZEOF([void *])
AC_CHECK_SIZEOF([char])
AC_CHECK_SIZEOF([unsigned char])
AC_CHECK_SIZEOF([signed char])
AC_CHECK_SIZEOF([short])
AC_CHECK_SIZEOF([unsigned short])
AC_CHECK_SIZEOF([int])
AC_CHECK_SIZEOF([unsigned int])
AC_CHECK_SIZEOF([long])
AC_CHECK_SIZEOF([unsigned long])
AS_IF([test x$ac_cv_type_long_long_int = xyes],
      [AC_CHECK_SIZEOF([long long])])
AS_IF([test x$ac_cv_type_unsigned_long_long_int = xyes],
      [AC_CHECK_SIZEOF([unsigned long long])])
AS_IF([test x$ac_cv_c_int64_t = xyes],
      [AC_CHECK_SIZEOF([int64_t])])
AS_IF([test x$ac_cv_c_uint64_t = xyes],
      [AC_CHECK_SIZEOF([uint64_t])])
AC_CHECK_SIZEOF([float])
AC_CHECK_SIZEOF([double])
AS_IF([test x$ac_cv_type_long_double = xyes],
      [AC_CHECK_SIZEOF([long double])])
AS_IF([test x$ac_cv_type_MPI_Offset = xyes],
      [AC_CHECK_SIZEOF([MPI_Offset], [], [[#include <mpi.h>]])])

# Checks for C++ library functions.
AC_CHECK_FUNCS([clock floor gettimeofday memset strtol])

# Checks for C++ libraries.
AC_SEARCH_LIBS([clock_gettime], [rt],
    [AC_DEFINE([HAVE_CLOCK_GETTIME], [1], [Define to 1 if you have the 'clock_gettime' function.])],
    [AC_DEFINE([HAVE_CLOCK_GETTIME], [0], [Define to 1 if you have the 'clock_gettime' function.])])

# MPI is required, probably for the foreseeable future.
PAGODA_CHECK_PACKAGE([mpi], [mpi.h], [mpi], [MPI_Init], [], [],
    [AC_MSG_FAILURE([Could not locate Message Passing Interface])])
AM_CONDITIONAL([HAVE_MPI], [test "x$pg_cv_mpi_found" = xyes])

# GA is required, for now.  Perhaps with MPI3 the GA could go...
PAGODA_CHECK_PACKAGE([ga], [ga.h], [ga], [GA_Initialize], [$FLIBS],
    [pg_ga_uses_flibs=yes],
    [AS_UNSET([ac_cv_search_GA_Initialize])
     PAGODA_CHECK_PACKAGE([ga], [ga.h], [ga], [GA_Initialize], [],
        [pg_ga_uses_flibs=no],
        [AS_UNSET([ac_cv_search_GA_Initialize])
         save_LIBS="$LIBS"
         PAGODA_CHECK_PACKAGE([ga], [ga.h], [global], [GA_Initialize],
            [-lpario -lglobal -lma -larmci -llinalg $FLIBS],
            [GA_LIBS="-lpario -lglobal -lma -larmci -llinalg"],
            [AC_MSG_FAILURE([Could not locate Global Arrays 5.x nor 4.x])])
         LIBS="$save_LIBS"])])
AM_CONDITIONAL([HAVE_GA], [test "x$pg_cv_ga_found" = xyes])
AM_CONDITIONAL([GA_USES_FLIBS], [test "x$pg_ga_uses_flibs" = xyes])

# Check for pnetcdf.
PAGODA_CHECK_PACKAGE([pnetcdf], [pnetcdf.h], [pnetcdf], [ncmpi_open])
AM_CONDITIONAL([HAVE_PNETCDF], [test "x$pg_cv_pnetcdf_found" = xyes])
AC_SUBST([have_pnetcdf], [$pg_cv_pnetcdf_found])

# Check for netcdf4.
PAGODA_CHECK_PACKAGE([netcdf4], [netcdf.h], [netcdf], [nc_open_par],
    [-lhdf5_hl -lhdf5 -lz -lm],
    [NETCDF4_LIBS="-lnetcdf -lhdf5_hl -lhdf5 -lz -lm"])
AM_CONDITIONAL([HAVE_NETCDF4], [test "x$pg_cv_netcdf4_found" = xyes])
AC_SUBST([have_netcdf4], [$pg_cv_netcdf4_found])

# We require at least one IO library.
AS_CASE([$pg_cv_pnetcdf_found:$pg_cv_netcdf4_found], [no:no],
        [AC_MSG_FAILURE([Could not locate an IO library])])

# Does pnetcdf have the new nonblocking interface?
PAGODA_PNETCDF_NB_INT

# Does pnetcdf have ncmpi_inq_format function?
PAGODA_PNETCDF_INQ_FORMAT

# Checks for GA functionality.
PAGODA_GA_GOP

AC_LANG_POP([C++])

#####################################################3
# Debug helper stuff.
#####################################################3
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

#####################################################3
# Misc.
#####################################################3

# Whether to build the sandbox programs.
AC_ARG_ENABLE([sandbox],
    [AS_HELP_STRING([--enable-sandbox], [build sandbox programs])],
    [],
    [enable_sandbox=no])
AM_CONDITIONAL([ENABLE_SANDBOX], [test "x$enable_sandbox" = xyes])
AC_SUBST([enable_sandbox])

# Whether to build Global Arrays programs within the sandbox.
AC_ARG_ENABLE([sandbox-ga],
    [AS_HELP_STRING([--enable-sandbox-ga],
                    [build Global Arrays sandbox programs])],
    [],
    [enable_sandbox_ga=no])
AM_CONDITIONAL([ENABLE_SANDBOX_GA], [test "x$enable_sandbox_ga" = xyes])
AC_SUBST([enable_sandbox_ga])

AC_ARG_VAR([MPIRUN], [how to launch MPI programs])
AC_PATH_PROG([MPIRUN], [mpirun])
AM_CONDITIONAL([HAVE_MPIRUN], [test "x$MPIRUN" != x])
AC_SUBST([MPIRUN])

AC_ARG_VAR([NP_FIRST], [min number of MPI processes to use (default=1)])
AC_ARG_VAR([NP_LAST], [max number of MPI processes to use (default=4)])
AC_ARG_VAR([NP_INC], [increment for number of MPI processes to use (default=1)])
AS_IF([test "x$NP_FIRST" = x], [NP_FIRST=1])
AS_IF([test "x$NP_LAST" = x], [NP_LAST=4])
AS_IF([test "x$NP_INC" = x], [NP_INC=1])
AC_SUBST([NP_FIRST])
AC_SUBST([NP_LAST])
AC_SUBST([NP_INC])

# NERSC's MPI library requires all C++ codes to include mpi.h before any
# other include files.  I'm not sure if this includes config.h as well, but
# putting this here will at least put mpi.h before any OTHER include files.
AH_BOTTOM([#include <mpi.h>])

#####################################################3
# Libtool.
#####################################################3
AC_MSG_NOTICE
AC_MSG_NOTICE([Libtool tests])
AC_MSG_NOTICE
LT_INIT

#####################################################3
# The End.
#####################################################3
AC_CONFIG_FILES([Makefile tests/atlocal])
AC_OUTPUT
