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
#include "FileWriter.H"

#if HAVE_PNETCDF
extern Dataset* pagoda_pnetcdf_open(const std::string&);
extern FileWriter* pagoda_pnetcdf_create(const std::string&);
#endif
#if HAVE_NETCDF4
extern Dataset* pagoda_netcdf4_open(const std::string&);
extern FileWriter* pagoda_netcdf4_create(const std::string&);
#endif


int pagoda::me = 0;
int pagoda::npe = 0;

extern "C" void pagoda_register_stack_memory();

void pagoda::initialize(int *argc, char ***argv)
{
#if HAVE_MPI
    MPI_Init(argc,argv);
#endif
#if HAVE_GA
    GA_Initialize();
    // avoid using MA in GA
    pagoda_register_stack_memory();
#endif
#if HAVE_GA
    pagoda::me = GA_Nodeid();
    pagoda::npe = GA_Nnodes();
#elif HAVE_MPI
    MPI_Comm_rank(MPI_COMM_WORLD, &pagoda::me);
    MPI_Comm_size(MPI_COMM_WORLD, &pagoda::npe);
#endif
#if HAVE_PNETCDF
    Dataset::register_opener(pagoda_pnetcdf_open);
    FileWriter::register_writer(pagoda_pnetcdf_create);
#endif
#if HAVE_NETCDF4
    Dataset::register_opener(pagoda_netcdf4_open);
    FileWriter::register_writer(pagoda_netcdf4_create);
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
