#####################################################3
# autoconf
#####################################################3
AC_PREREQ([2.64])
AC_INIT([pagoda], [0.2], [jeff.daily@pnl.gov])
AC_CONFIG_SRCDIR([Mainp.C])
AC_CONFIG_HEADERS([config/config.h])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([config])

#####################################################3
# automake
#####################################################3
AM_INIT_AUTOMAKE([silent-rules])
AM_DISABLE_SHARED

#####################################################3
# Fortran 77
#####################################################3
AC_LANG_PUSH([Fortran 77])

# Checks for Fortran 77 programs.
AC_PROG_F77([mpif77 hf77 mpxlf_r mpxlf mpf77 cmpifc ftn])

# Checks for Fortran 77 header files.

# Checks for Fortran 77 typedefs, structures, and compiler characteristics.

# Checks for Fortran 77 type sizes.

# Checks for Fortran 77 library functions.

# Checks for Fortran 77 libraries.

AC_LANG_POP([Fortran 77])

#####################################################3
# C
#####################################################3
AC_LANG_PUSH([C])

# Checks for C programs.
AC_PROG_CC([mpicc hcc mpxlc_r mpxlc mpcc cmpicc cc])

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
# C++
#####################################################3
AC_LANG_PUSH([C++])

# Checks for C++ programs.
AC_PROG_CXX([mpic++ mpicxx mpiCC hcp mpxlC_r mpxlC mpCC cmpic++ CC])

# Checks for C++ header files.

# Checks for C++ typedefs, structures, and compiler characteristics.
AC_F77_LIBRARY_LDFLAGS
AC_F77_DUMMY_MAIN
AC_TYPE_LONG_LONG_INT
AC_TYPE_UNSIGNED_LONG_LONG_INT
AC_TYPE_INT64_T
AC_TYPE_UINT64_T
AC_CHECK_TYPE([MPI_Offset], [], [], [[#include <mpi.h>]])
AC_TYPE_LONG_DOUBLE
GCRM_CXX_VARIADIC_MACROS([],
    [AC_MSG_WARN([C99 variadic macros not supported, IO timing disabled])])

# Checks for C++ type sizes.
AC_CHECK_SIZEOF([void *])
AC_CHECK_SIZEOF([char])
AC_CHECK_SIZEOF([unsigned char])
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
AC_CHECK_FUNCS([clock clock_gettime floor gettimeofday memset strtol])

# Checks for C++ libraries.
GCRM_CHECK_PACKAGE([pnetcdf], [pnetcdf.h], [pnetcdf], [ncmpi_open], [], [],
    [AC_MSG_FAILURE([Could not locate pnetcdf])])
GCRM_CHECK_PACKAGE([ga], [ga.h], [ga], [GA_Initialize], [$FLIBS], [],
    [GCRM_CHECK_PACKAGE([ga], [ga.h], [global], [GA_Initialize], [$FLIBS], [],
        [AC_MSG_FAILURE([Could not locate ga5x nor ga4x])])])

AC_LANG_POP([C++])

#####################################################3
# Libtool.
#####################################################3
LT_INIT

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
# NERSC's MPI library requires all C++ codes to include mpi.h before any
# other include files.  I'm not sure if this includes config.h as well, but
# putting this here will at least put mpi.h before any OTHER include files.
AH_BOTTOM([#include <mpi.h>])

#####################################################3
# The End.
#####################################################3
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
