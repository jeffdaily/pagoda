/**
 * This program helped me determine whether pnetcdf maintains a single
 * integer handle per open file. The results were that no, each ncmpi_open
 * generates the next available positive integer handle.
 * Closing open files releases their particular handle.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif // HAVE_CONFIG_H

// C includes, std and otherwise
#include <ga.h>
#include <macdecls.h>
#include <mpi.h>
#include <pnetcdf.h>

// C++ includes, std and otherwise
#include <iostream>
using std::cout;
using std::endl;

// C++ includes
#include "Util.H"


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

