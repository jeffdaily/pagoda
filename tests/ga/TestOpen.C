#include <stdio.h>
#include <mpi.h>
#include <pnetcdf.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    if (argc < 2) return 1;

    int error, one;
    error = ncmpi_open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &one);
    if (NC_NOERR != error)  {
        printf("%s\n", ncmpi_strerror(error));
    }

    error = ncmpi_close(one);
    if (NC_NOERR != error)  {
        printf("%s\n", ncmpi_strerror(error));
    }

    // Must always call to exit cleanly.
    MPI_Finalize();

    return 0;
}

