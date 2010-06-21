#include "pagoda.H"

#if HAVE_MPI
#   include <mpi.h>
#endif
#if HAVE_GA
#   include <ga.h>
#endif


void pagoda::initialize(int *argc, char ***argv)
{
#if HAVE_MPI
    MPI_Init(argc,argv);
#endif
#if HAVE_GA
    GA_Initialize();
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
