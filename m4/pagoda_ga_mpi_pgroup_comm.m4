# PAGODA_GA_MPI_PGROUP_COMMUNICATOR
# ---------------------------------
# Whether the GA library implements ga_mpi_pgroup_communicator(id).
AC_DEFUN([PAGODA_GA_MPI_PGROUP_COMMUNICATOR],
[save_LDFLAGS="$LDFLAGS"; LDFLAGS="$LDFLAGS $GA_LDFLAGS"
save_LIBS="$LIBS"; LIBS="$LIBS $GA_LIBS $FLIBS"
AC_CHECK_FUNCS([GA_MPI_Comm_pgroup])
LDFLAGS="$save_LDFLAGS"
LIBS="$save_LIBS"])
])dnl
