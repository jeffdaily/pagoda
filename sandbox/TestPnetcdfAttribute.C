#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include <cstdlib>
#include <iostream>

#include <Pnetcdf.H>


int main(int argc, char **argv)
{
    MPI_Init(&argc,&argv);

    if (argc != 2) {
        std::cerr << "Usage: TestPnetcdfAttribute <outfile>" << std::endl;
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    int ncid = ncmpi::create(MPI_COMM_WORLD, argv[1],
            NC_64BIT_OFFSET, MPI_INFO_NULL);
    ncmpi::put_att(ncid, NC_GLOBAL, "testing", "aaaaaa");
    ncmpi::put_att(ncid, NC_GLOBAL, "testing", "aaaaaabbbbbb");
    ncmpi::put_att(ncid, NC_GLOBAL, "testing", "ccc");
    ncmpi::close(ncid);

    MPI_Finalize();
}
