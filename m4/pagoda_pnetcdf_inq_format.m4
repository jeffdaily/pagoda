# PAGODA_PNETCDF_INQ_FORMAT([ACTION-IF-TRUE], [ACTION-IF-FALSE])
# --------------------------------------------------------------
# Whether the pnetcdf library has the inq_format function.
# It was added in 1.2 (it's not in 1.1.1 or earlier, AFAIK)
AC_DEFUN([PAGODA_PNETCDF_INQ_FORMAT],
[save_CPPFLAGS="$CPPFLAGS"; CPPFLAGS="$CPPFLAGS $PNETCDF_CPPFLAGS"
AC_CHECK_DECLS([ncmpi_inq_format], [], [], [#include <pnetcdf.h>])
CPPFLAGS="$save_CPPFLAGS"
save_LDFLAGS="$LDFLAGS"; LDFLAGS="$LDFLAGS $PNETCDF_LDFLAGS"
save_LIBS="$LIBS"; LIBS="$PNETCDF_LIBS $LIBS"
AC_CHECK_FUNCS([ncmpi_inq_format])
LDFLAGS="$save_LDFLAGS"
LIBS="$save_LIBS"
])dnl
