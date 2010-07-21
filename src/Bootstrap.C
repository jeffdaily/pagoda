#if HAVE_CONFIG_H
#   include <config.h>
#endif

#if HAVE_MPI
#   include <mpi.h>
#endif
#if HAVE_GA
#   include <ga.h>
#endif

#include "Bootstrap.H"


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
