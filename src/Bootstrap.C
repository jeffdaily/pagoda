#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <string>

#if HAVE_MPI
#   include <mpi.h>
#endif
#if HAVE_GA
#   include <ga.h>
#endif

#include "Bootstrap.H"
#include "Dataset.H"

#if HAVE_PNETCDF
extern Dataset* pagoda_pnetcdf_open(const std::string&);
#endif
#if HAVE_NETCDF4
extern Dataset* pagoda_netcdf4_open(const std::string&);
#endif


int pagoda::me = 0;
int pagoda::npe = 0;

extern "C" void pagoda_register_stack_memory();

void pagoda::initialize(int *argc, char ***argv)
{
#if HAVE_MPI
    MPI_Init(argc,argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pagoda::me);
    MPI_Comm_size(MPI_COMM_WORLD, &pagoda::npe);
#endif
#if HAVE_GA
    GA_Initialize();
    // avoid using MA in GA
    pagoda_register_stack_memory();
    pagoda::me = GA_Nodeid();
    pagoda::npe = GA_Nnodes();
#endif
#if HAVE_PNETCDF
    Dataset::register_opener(pagoda_pnetcdf_open);
#endif
#if HAVE_NETCDF4
    Dataset::register_opener(pagoda_netcdf4_open);
#endif
}


void pagoda::finalize()
{
#if HAVE_GA
    GA_Terminate();
#endif
#if HAVE_MPI
    MPI_Finalize();
#endif
}
