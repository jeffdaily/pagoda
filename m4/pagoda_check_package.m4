# PAGODA_CHECK_PACKAGE(pkg, header, library, function, [extra-libs],
#                      [action-if-found], [action-if-not-found])
# -----------------------------------------------------------------
#
AC_DEFUN([PAGODA_CHECK_PACKAGE], [
AS_VAR_PUSHDEF([PKG_CACHE],   m4_tolower(m4_translit([pg_cv_$1_found], [-.], [__])))
AS_VAR_PUSHDEF([HAVE_PKG],    m4_toupper(m4_translit([HAVE_$1], [-.], [__])))
AS_VAR_PUSHDEF([PKG_LIBS],    m4_toupper(m4_translit([$1_LIBS], [-.], [__])))
AS_VAR_PUSHDEF([PKG_LDFLAGS], m4_toupper(m4_translit([$1_LDFLAGS], [-.], [__])))
AS_VAR_PUSHDEF([PKG_CPPFLAGS],m4_toupper(m4_translit([$1_CPPFLAGS], [-.], [__])))
AS_VAR_SET([PKG_LIBS],[])
AS_VAR_SET([PKG_LDFLAGS],[])
AS_VAR_SET([PKG_CPPFLAGS],[])
AC_ARG_WITH([$1],
    [AS_HELP_STRING([--with-$1[[=ARG]]],
        [specify location of $1 install and/or other flags])],
    [],
    [with_$1=yes])
AS_CASE([$with_$1],
    [yes],  [],
    [no],   [AS_VAR_SET([PKG_CACHE],[skipped])],
            [PAGODA_ARG_PARSE(
                [with_$1],
                [PKG_LIBS],
                [PKG_LDFLAGS],
                [PKG_CPPFLAGS])])
AS_VAR_IF([PKG_CACHE], ["skipped"], [ac_cv_search_$4=no], [
# Save user variables.
pagoda_save_CPPFLAGS="$CPPFLAGS"
pagoda_save_LDFLAGS="$LDFLAGS"
pagoda_save_LIBS="$LIBS"
# Check for header.
# Always add user-supplied PKG_CPPFLAGS to CPPFLAGS first.
CPPFLAGS="$CPPFLAGS $PKG_CPPFLAGS"
AC_CHECK_HEADER([$2], [], [$7])
# Restore user variable.
CPPFLAGS="$pagoda_save_CPPFLAGS"
# Check for library.
# Always add user-supplied PKG_LDFLAGS and PKG_LIBS.
LDFLAGS="$LDFLAGS $PKG_LDFLAGS"
LIBS="$PKG_LIBS $LIBS"
AC_SEARCH_LIBS([$4], [$3], [], [], [$5])
# If a library was required and found, ac_res is set appropriately.
AS_IF([test "x$ac_res" != xno],
    [AS_IF([test "x$ac_res" != "none required"],
        [PKG_LIBS="$ac_res"])])
# Restore user variables.
LDFLAGS="$pagoda_save_LDFLAGS"
LIBS="$pagoda_save_LIBS"
])
AC_SUBST(PKG_LIBS)
AC_SUBST(PKG_LDFLAGS)
AC_SUBST(PKG_CPPFLAGS)
AS_IF([test "x$ac_cv_search_$4" != xno],
    [$6
     AC_DEFINE([HAVE_PKG], [1],
        [set to 1 if we have the indicated package])
     AS_VAR_SET([PKG_CACHE],[yes])],
    [$7
     AS_VAR_SET([PKG_CACHE],[no])])
AS_VAR_POPDEF([PKG_CACHE])
AS_VAR_POPDEF([HAVE_PKG])
AS_VAR_POPDEF([PKG_LIBS])
AS_VAR_POPDEF([PKG_LDFLAGS])
AS_VAR_POPDEF([PKG_CPPFLAGS])
])dnl
