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
#include "Dimension.H"
#include "NetcdfDimension.H"
#include "SubsetterException.H"
#include "Util.H"


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();

    if (argc < 2) return EXIT_FAILURE;

    for (int argi=0; argi<argc; ++argi) {
        cout << argv[argi] << endl;
    }

    int err, ncid, ndim, nvar;
    err = ncmpi_open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &ncid);
    ERRNO_CHECK(err);

    err = ncmpi_inq_ndims(ncid, &ndim);
    ERRNO_CHECK(err);

    for (int dimid=0; dimid<ndim; ++dimid) {
        Dimension *dim = new NetcdfDimension(ncid, dimid);
        cout << dim << endl;
        delete dim;
    }

    err = ncmpi_close(ncid);
    ERRNO_CHECK(err);

    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return EXIT_SUCCESS;
}

