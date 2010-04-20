# GCRM_CHECK_PACKAGE(prefix, header, library, function, extra-libs)
# -----------------------------------------------------------------
#
AC_DEFUN([GCRM_CHECK_PACKAGE], [
$1_LIBS=
$1_LDFLAGS=
$1_CPPFLAGS=
gcrm_check_$1_save_LIBS="$LIBS"
gcrm_check_$1_save_LDFLAGS="$LDFLAGS"
gcrm_check_$1_save_CPPFLAGS="$CPPFLAGS"
AC_ARG_WITH([$1],
    [AS_HELP_STRING([--with-$1[[=ARG]]],
        [specify location of $1 install and/or other flags])],
    [],
    [with_$1=yes])
AS_CASE([$with_$1],
    [yes],  [],
    [no],   [AC_MSG_ERROR([--without-$1 is not allowed])],
            [GCRM_ARG_PARSE([with_$1], [$1_LIBS],
                [$1_LDFLAGS], [$1_CPPFLAGS])])

# Check for header.
# Add $1_CPPFLAGS to CPPFLAGS first.
CPPFLAGS="$CPPFLAGS $$1_CPPFLAGS"
AC_CHECK_HEADER([$2], [],
    [AC_MSG_FAILURE([$2 is required])])
CPPFLAGS="$gcrm_check_$1_save_CPPFLAGS"

# Check for library.
# Always add user-supplied $1_LDFLAGS.
LDFLAGS="$LDFLAGS $$1_LDFLAGS"
# If user supplied LIBS, use those.  Augment with $3, if missing.
LIBS="$$1_LIBS $5 $LIBS"
AS_CASE([$LIBS], [*$3*], [], [LIBS="-l$3 $LIBS"; $1_LIBS="-l$3 $$1_LIBS"])
AS_LITERAL_IF([$3],
    [AS_VAR_PUSHDEF([gcrm_Lib], [ac_cv_lib_$3_$4])],
    [AS_VAR_PUSHDEF([gcrm_Lib], [ac_cv_lib_$3''_$4])])dnl
AC_CACHE_CHECK([for $4 in -l$3], [gcrm_Lib],
   [AC_LINK_IFELSE([AC_LANG_CALL([], [$4])],
       [AS_VAR_SET([gcrm_Lib], [yes])],
       [AS_VAR_SET([gcrm_Lib], [no])])])
LIBS="$gcrm_check_$1_save_LIBS"
AS_IF([test "x$gcrm_Lib" = xno],
    [AC_MSG_FAILURE([Could not link -l$3])])
LDFLAGS="$gcrm_check_$1_save_LDFLAGS"
AS_VAR_POPDEF([gcrm_Lib])

AC_SUBST([$1_LIBS])
AC_SUBST([$1_LDFLAGS])
AC_SUBST([$1_CPPFLAGS])

])dnl
