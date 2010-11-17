# PAGODA_PNETCDF_NB_INT([ACTION-IF-TRUE], [ACTION-IF-FALSE])
# ------------------------------------------------------------
# Whether the pnetcdf library uses the int non-blocking interface.
# In 1.2 it changed the API from NCMPI_Request to int as well as changing the
# ncmpi_waitall function to ncmpi_wait_all.
AC_DEFUN([PAGODA_PNETCDF_NB_INT],
[AC_CACHE_CHECK([whether pnetcdf uses the int non-blocking interface],
    [pagoda_cv_pnetcdf_nb_int],
    [save_LDFLAGS="$LDFLAGS"; LDFLAGS="$LDFLAGS $PNETCDF_LDFLAGS"
     save_LIBS="$LIBS"; LIBS="$LIBS $PNETCDF_LIBS"
     AC_LANG_PUSH([C++])
     AC_LINK_IFELSE([AC_LANG_CALL([], [ncmpi_wait_all])],
        [pagoda_cv_pnetcdf_nb_int=yes],
        [pagoda_cv_pnetcdf_nb_int=no])
     AC_LANG_POP([C++])
     LDFLAGS="$save_LDFLAGS"
     LIBS="$save_LIBS"])
AS_IF([test $pagoda_cv_pnetcdf_nb_int = yes],
    [$1
     AC_DEFINE([HAVE_PNETCDF_NEW_NB], [1], [pnetcdf version >=1.2])],
    [$2])
])dnl
