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
AC_PROG_FC([mpif90 mpxlf95_r mpxlf90_r mpxlf95 mpxlf90 mpf90 cmpif90c ftn])
AC_FC_LIBRARY_LDFLAGS

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

#GCRM_CHECK_PACKAGE([ga], [NGA_Create], [global], [ga.h],
#  [AC_SUBST([LIBGA], [-lglobal -lma -llinalg -larmci -lm])
#   AC_DEFINE([HAVE_LIBGA], [1], [Define if you have Global Arrays])
#  ],
#  [AC_MSG_FAILURE([--with-ga was given, but test for ga failed])],
#  [-lma -llinalg -larmci -lm]
#)

AC_ARG_VAR([GA_INCS], [Global Arrays include directives])
AC_SUBST([GA_INCS])
AC_ARG_VAR([GA_LIBS], [Global Arrays C linker directives])
AC_SUBST([GA_LIBS])

# Checks for header files.
AC_CHECK_HEADERS([limits.h stdlib.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_TYPE_INT64_T
AC_TYPE_SIZE_T

# Checks for library functions.
AC_CHECK_FUNCS([strtol])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

