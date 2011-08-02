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
#include "Print.H"
#include "ProcessGroup.H"

#if HAVE_PNETCDF
extern Dataset* pagoda_pnetcdf_open(const std::string&, const ProcessGroup&);
extern FileWriter* pagoda_pnetcdf_create(const std::string&, FileFormat);
#   include "PnetcdfTiming.H"
#   include "Util.H"
#endif
#if HAVE_NETCDF4
extern Dataset* pagoda_netcdf4_open(const std::string&, const ProcessGroup&);
extern FileWriter* pagoda_netcdf4_create(const std::string&, FileFormat);
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
#if HAVE_PNETCDF && defined(GATHER_PNETCDF_TIMING)
    PnetcdfTiming::start_global = PnetcdfTiming::get_time();
#endif
}


void pagoda::finalize()
{
#if HAVE_PNETCDF && defined(GATHER_PNETCDF_TIMING)
    PnetcdfTiming::end_global = PnetcdfTiming::get_time();
    pagoda::println_zero("PnetcdfTiming::end_global  ="
            + pagoda::to_string(PnetcdfTiming::end_global));
    pagoda::println_zero("PnetcdfTiming::start_global="
            + pagoda::to_string(PnetcdfTiming::start_global));
    pagoda::println_zero("                diff_global="
            + pagoda::to_string(PnetcdfTiming::end_global
                -PnetcdfTiming::start_global));
    pagoda::println_zero(PnetcdfTiming::get_stats_calls());
    pagoda::println_zero(PnetcdfTiming::get_stats_aggregate());
#endif
#if HAVE_GA
    GA_Terminate();
#endif
#if HAVE_MPI
    MPI_Finalize();
#endif
}
