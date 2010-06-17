# GCRM_CHECK_PACKAGE(prefix, header, library, function, [extra-libs],
#                    [action-if-found], [action-if-not-found])
# -----------------------------------------------------------------
#
AC_DEFUN([GCRM_CHECK_PACKAGE], [
$1_LIBS=
$1_LDFLAGS=
$1_CPPFLAGS=
AC_ARG_WITH([$1],
    [AS_HELP_STRING([--with-$1[[=ARG]]],
        [specify location of $1 install and/or other flags])],
    [],
    [with_$1=yes])
AS_CASE([$with_$1],
    [yes],  [],
    [no],   [],
            [GCRM_ARG_PARSE([with_$1], [$1_LIBS], [$1_LDFLAGS], [$1_CPPFLAGS])])
# Check for header.
# Add $1_CPPFLAGS to CPPFLAGS first.
CPPFLAGS="$CPPFLAGS $$1_CPPFLAGS"
AC_CHECK_HEADER([$2], [], [$7])
# Check for library.
# Always add user-supplied $1_LDFLAGS and $1_LIBS.
LDFLAGS="$LDFLAGS $$1_LDFLAGS"
LIBS="$$1_LIBS $LIBS"
AC_SEARCH_LIBS([$4], [$3], [], [], [$5])
AC_SUBST([$1_LIBS])
AC_SUBST([$1_LDFLAGS])
AC_SUBST([$1_CPPFLAGS])
AS_IF([test "x$ac_cv_search_$4" != xno],
    [$6
     AS_VAR_PUSHDEF([HAVE_PREFIX],
        m4_toupper(m4_translit([HAVE_$1], [-.], [__])))
     AC_DEFINE([HAVE_PREFIX], [1], [set to 1 if we have the indicated package])
     AS_VAR_POPDEF([HAVE_PREFIX])],
    [$7])
])dnl
