# PAGODA_CHECK_FLIBS
# ------------------
# Confirms that FLIBS does indeed work.
# Perhaps FLIBS isn't needed at all for linking.
AC_DEFUN([PAGODA_CHECK_FLIBS], [
AC_CACHE_CHECK([whether FLIBS works], [pg_cv_flibs_works],
    [AC_LANG_PUSH([Fortran 77])
     AC_COMPILE_IFELSE(
[[      subroutine foo
      end]],
        [mv conftest.$ac_objext conftestf.$ac_objext
         pg_save_LIBS="$LIBS"
         LIBS="conftestf.$ac_objext $FLIBS"
         AC_F77_FUNC([foo])
         AC_LANG_PUSH([C++])
         AC_LINK_IFELSE(
            [AC_LANG_CALL([],[$foo])],
            [pg_cv_flibs_works=yes],
            [pg_cv_flibs_works=no])
         AC_LANG_POP([C++])
         LIBS="$pg_save_LIBS"
         rm -f conftestf.$ac_objext],
        [pg_cv_flibs_works=no])
     AC_LANG_POP([Fortran 77])])
])dnl
