/**
 * An incredibly naive approach for helping determine a distance-weighted
 * average.
 *
 * A fundamental problem is finding nearest neighbors.  There are certainly
 * more efficient algorithms.  This is a brute force method.  Every cell
 * calculates its distance from a destination grid cell.
 *
 * This code exists to dis/prove whether this method is tractable.
 */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <mpi.h>
#include <ga.h>
#include <macdecls.h>
#include <pnetcdf.h>

#include <iostream>

#include "Debug.H"

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();

    if (3 != argc) {
        PRINT_ZERO("Usage: TestDistance <filename> <variablename>\n");
        for (int i=0; i<argc; ++i) {
            PRINT_ZERO("argv[%d]=%s\n", i, argv[i]);
        }
        return 1;
    }

    GA_Terminate();
    MPI_Finalize();

    return 0;
}
