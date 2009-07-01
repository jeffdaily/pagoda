#ifdef HAVE_CONFIG_H
#include "config.h"
#endif // HAVE_CONFIG_H

// C includes, std and otherwise
#include <ga.h>
#include <macdecls.h>
#include <mpi.h>
#include <pnetcdf.h>
#include <unistd.h>

// C++ includes, std and otherwise
#include <iostream>
using std::cout;
using std::endl;
#include <sstream>
using std::ostringstream;
#include <string>
using std::string;
#include <vector>
using std::vector;

// C++ includes
#include "Attribute.H"
#include "NetcdfAttribute.H"
#include "NetcdfVariable.H"
#include "SubsetterException.H"
#include "Variable.H"


#define ME GA_Nodeid()
int err;


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

    if (argc < 2) return EXIT_FAILURE;

    for (int argi=0; argi<argc; ++argi) {
        cout << argv[argi] << endl;
    }

    int ncid, natt, nvar;
    err = ncmpi_open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &ncid);
    ERRNO_CHECK(err);

    err = ncmpi_inq_natts(ncid, &natt);
    ERRNO_CHECK(err);

    cout << "@@@@@@@@@@@@@@@@@@@ GLOBAL ATTRIBUTES" << endl;
    for (int attid=0; attid<natt; ++attid) {
        Attribute *att = new NetcdfAttribute(ncid, attid, NC_GLOBAL);
        cout << att << endl;
        delete att;
    }

    err = ncmpi_inq_nvars(ncid, &nvar);
    ERRNO_CHECK(err);

    cout << "@@@@@@@@@@@@@@@@@@@ VARIABLE ATTRIBUTES" << endl;
    for (int varid=0; varid<nvar; ++varid) {
        Variable *var = new NetcdfVariable(ncid, varid);
        cout << var->get_name() << endl;
        vector<Attribute*> atts = var->get_atts();
        for (int attid=0,limit=atts.size(); attid<limit; ++attid) {
            cout << "\t" << atts.at(attid) << endl;
        }
        delete var;
    }

    err = ncmpi_close(ncid);
    ERRNO_CHECK(err);

    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return EXIT_SUCCESS;
}

