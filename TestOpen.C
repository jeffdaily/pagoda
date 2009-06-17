#ifdef HAVE_CONFIG_H
#include "config.h"
#endif // HAVE_CONFIG_H

// C includes, std and otherwise
#include <ga.h>
#include <limits.h> // for INT_MAX
#include <macdecls.h>
#include <math.h> // for M_PI
#include <mpi.h>
#include <pnetcdf.h>
#include <unistd.h>

// C++ includes, std and otherwise
#include <algorithm>
using std::copy;
using std::fill;
#include <iostream>
using std::cout;
using std::endl;
#include <map>
using std::make_pair;
using std::map;
using std::multimap;
#include <sstream>
using std::ostringstream;
#include <string>
using std::string;
#include <vector>
using std::vector;

// C++ includes
#include "DimensionSlice.H"
#include "LatLonBox.H"
#include "RangeException.H"
#include "SubsetterException.H"


static double DEG2RAD = M_PI / 180.0;
//static double RAD2DEG = 180.0 / M_PI;
static int ZERO = 0;
static int64_t ZERO64 = 0;
static int ONE = 1;
static string COMPOSITE_PREFIX("GCRM_COMPOSITE");


#define ME GA_Nodeid()
int error;


#define DEBUG
#ifdef DEBUG
#include <assert.h>
//#define DEBUG_MASKS
#define DEBUG_PRINT fprintf
#define DEBUG_PRINT_ME if (ME == 0) fprintf
#else
#define DEBUG_PRINT
#define DEBUG_PRINT_ME
#endif


#define MAX_NAME 80
char GOP_SUM[] = "+";
char NAME_VAR_IN[] = "var_in";
char NAME_VAR_OUT[] = "var_out";


#define ERR(e) { \
ostringstream __os; \
__os << "Error: " << e << endl; \
throw SubsetterException(__os.str()); }

#define ERRNO(n) { \
ostringstream __os; \
__os << "Error: " << ncmpi_strerror(n) << endl; \
throw SubsetterException(__os.str()); }

#define ERRNO_CHECK(n) \
  if (n != NC_NOERR) { \
    ERRNO(n); \
  }


#ifdef F77_DUMMY_MAIN
#  ifdef __cplusplus
     extern "C"
#  endif
   int F77_DUMMY_MAIN() { return 1; }
#endif


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();
    DEBUG_PRINT_ME(stderr, "After GA_Initialize\n");

    if (argc < 3) return EXIT_FAILURE;

    for (int argi=0; argi<argc; ++argi) {
        cout << argv[argi] << endl;
    }

    int error, one, two, three;
    error = ncmpi_open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &one);
    ERRNO_CHECK(error);
    error = ncmpi_open(MPI_COMM_WORLD, argv[2], NC_NOWRITE, MPI_INFO_NULL, &two);
    ERRNO_CHECK(error);
    error = ncmpi_open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &three);
    ERRNO_CHECK(error);
    cout << one << " " << argv[1] << endl;
    cout << two << " " << argv[2] << endl;
    cout << three << " " << argv[1] << endl;
    cout << "after close two" << endl;
    error = ncmpi_close(two);
    ERRNO_CHECK(error);
    cout << one << " " << argv[1] << endl;
    cout << two << " " << argv[2] << endl;
    cout << three << " " << argv[1] << endl;
    error = ncmpi_open(MPI_COMM_WORLD, argv[2], NC_NOWRITE, MPI_INFO_NULL, &two);
    ERRNO_CHECK(error);
    cout << "after reopen two" << endl;
    cout << one << " " << argv[1] << endl;
    cout << two << " " << argv[2] << endl;
    cout << three << " " << argv[1] << endl;

    error = ncmpi_close(one);
    ERRNO_CHECK(error);
    error = ncmpi_close(two);
    ERRNO_CHECK(error);
    error = ncmpi_close(three);
    ERRNO_CHECK(error);

    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return EXIT_SUCCESS;
}

