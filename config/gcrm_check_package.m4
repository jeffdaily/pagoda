# ===========================================================================
#
# SYNOPSIS
#
#   GCRM_CHECK_PACKAGE(PACKAGE, FUNCTION, LIBRARY , HEADERFILE [, ACTION-IF-FOUND [, ACTION-IF-NOT-FOUND [, OTHER-LIBRARIES ]]])
#
# DESCRIPTION
#
#   Heavily modified version of AC_caolan_CHECK_PACKAGE from autoconf archives.
#
#   Provides --with-PACKAGE, --with-PACKAGE-include and
#   --with-PACKAGE-lib options to configure. Supports the now standard
#   --with-PACKAGE=DIR approach where the package's include dir and lib dir
#   are underneath DIR, but also allows the include and lib directories to
#   be specified seperately
#
#   adds the extra -Ipath to CFLAGS if needed adds extra -Lpath to LD_FLAGS
#   if needed searches for the FUNCTION in the LIBRARY with AC_CHECK_LIBRARY
#   and thus adds the lib to LIBS
#
#   defines HAVE_PKG_PACKAGE if it is found, (where PACKAGE in the
#   HAVE_PKG_PACKAGE is replaced with the actual first parameter passed)
#   note that autoheader will complain of not having the HAVE_PKG_PACKAGE
#   and you will have to add it to acconfig.h manually
#
# LICENSE
#
#   Copyright (c) 2008 Caolan McNamara <caolan@skynet.ie>
#   Copyright (c) 2008 Alexandre Duret-Lutz <adl@gnu.org>
#   Copyright (c) 2008 Matthew Mueller <donut@azstarnet.com>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved.

AC_DEFUN([GCRM_CHECK_PACKAGE],
[

AC_ARG_WITH($1,
  [AS_HELP_STRING([--with-$1[[=DIR]]],
    [root directory of $1 installation])],
  [],
  [with_$1=yes])
if test "${with_$1}" != yes -a "${with_$1}" != no ; then
  $1_include="${with_$1}/include"
  $1_lib="${with_$1}/lib"
fi

AC_ARG_WITH($1-include,
  [AS_HELP_STRING([--with-$1-include=DIR],
    [specify exact include dir for $1 headers])],
  [],
  [with_$1_include=no])
if test "${with_$1_include}" = yes ; then
  AC_MSG_ERROR([You must specify DIR for --with-$1-include=DIR])
fi
if test "${with_$1_include}" != no ; then
  $1_include="${with_$1_include}"
  if test "${with_$1}" = no ; then
    with_$1=yes
  fi
fi

AC_ARG_WITH($1-lib,
  [AS_HELP_STRING([--with-$1-lib=DIR],
    [specify exact library dir for $1 library])],
  [],
  [with_$1_lib=no])
if test "${with_$1_lib}" = yes ; then
  AC_MSG_ERROR([You must specify DIR for --with-$1-lib=DIR])
fi
if test "${with_$1_lib}" != no ; then
  $1_lib="${with_$1_lib}"
  if test "${with_$1}" = no ; then
    with_$1=yes
  fi
fi

if test "${with_$1}" != no ; then
  OLD_LIBS=$LIBS
  OLD_LDFLAGS=$LDFLAGS
  OLD_CFLAGS=$CFLAGS
  
  if test "x${$1_lib}" != x ; then
    LDFLAGS="$LDFLAGS -L${$1_lib}"
  fi
  if test "x${$1_include}" != x ; then
    CFLAGS="$CFLAGS -I${$1_include}"
  fi
  
  no_good=no
  AC_CHECK_LIB($3,$2,,no_good=yes,$7)
  AC_CHECK_HEADER($4,,no_good=yes)
  if test "$no_good" = yes; then
dnl broken
    ifelse([$6], , , [$6])
    LIBS=$OLD_LIBS
    LDFLAGS=$OLD_LDFLAGS
    CFLAGS=$OLD_CFLAGS
  else
dnl fixed
    ifelse([$5], , , [$5])
dnl    AC_DEFINE(HAVE_PKG_$1)
  fi
fi

])
