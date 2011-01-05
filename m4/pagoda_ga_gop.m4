# PAGODA_GA_GOP
# -------------
# Whether the GA library implements GA_Llgop and GA_Ldgop.
AC_DEFUN([PAGODA_GA_GOP],
[save_LDFLAGS="$LDFLAGS"; LDFLAGS="$LDFLAGS $GA_LDFLAGS"
save_LIBS="$LIBS"; LIBS="$LIBS $GA_LIBS $FLIBS"
AC_CHECK_FUNCS([GA_Llgop GA_Ldgop])
LDFLAGS="$save_LDFLAGS"
LIBS="$save_LIBS"])
])dnl
