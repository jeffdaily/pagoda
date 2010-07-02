# PAGODA_CXX_VARIADIC_MACROS([ACTION-IF-TRUE], [ACTION-IF-FALSE])
# ------------------------------------------------------------
# Whether the CXX preprocessor understands C99 variadic macros.
AC_DEFUN([PAGODA_CXX_VARIADIC_MACROS],
[AC_CACHE_CHECK([whether the CXX compiler understands C99 variadic macros],
    [gcrm_cv_cxx_variadic_macros],
    [AC_LANG_PUSH([C++])
    AC_PREPROC_IFELSE(
        [AC_LANG_PROGRAM(
            [[#include <cstdio>
using namespace std;
#define myprintf(...) fprintf(stderr, __VA_ARGS__)]],
            [[myprintf("hello %d\n", 3);]])],
        [gcrm_cv_cxx_variadic_macros=yes],
        [gcrm_cv_cxx_variadic_macros=no])
    AC_LANG_POP([C++])])
AS_IF([test $gcrm_cv_cxx_variadic_macros = yes], [$1], [$2])
])dnl
