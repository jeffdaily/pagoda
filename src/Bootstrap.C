#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <string>

#include <mpi.h>
#if HAVE_GA
#   include <ga.h>
#endif
#if HAVE_BIL
#   include <bil.h>
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
MPI_Comm pagoda::COMM_WORLD = MPI_COMM_NULL;

extern "C" void pagoda_register_stack_memory();

void pagoda::initialize(int *argc, char ***argv)
{
    int mpierr;
    MPI_Init(argc,argv);
    mpierr = MPI_Comm_dup(MPI_COMM_WORLD, &pagoda::COMM_WORLD);
    if (mpierr != MPI_SUCCESS) {
        MPI_Abort(MPI_COMM_WORLD, -6);
    }
#if HAVE_BIL
    BIL_Init(pagoda::COMM_WORLD);
#endif
#if HAVE_GA
    GA_Initialize();
    // avoid using MA in GA
    pagoda_register_stack_memory();
#endif
#if HAVE_GA
    pagoda::me = GA_Nodeid();
    pagoda::npe = GA_Nnodes();
#else
    MPI_Comm_rank(pagoda::COMM_WORLD, &pagoda::me);
    MPI_Comm_size(pagoda::COMM_WORLD, &pagoda::npe);
#endif
    ProcessGroup::set_default(ProcessGroup::get_world());
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
    MPI_Finalize();
}


/**
 * Abort the parallel application.
 *
 * @param[in] message message to print before aborting
 */
void pagoda::abort(const string &message)
{
#if HAVE_GA
    GA_Error(const_cast<char*>(message.c_str()), 1);
#elif HAVE_MPI
    cerr << "[" << nodeid() << "] " << message << endl;
    MPI_Abort(MPI_COMM_WORLD, 1);
#else
#   error
#endif
}


/**
 * Abort the parallel application.
 *
 * @param[in] message message to print before aborting
 * @param[in] errorcode
 */
void pagoda::abort(const string &message, int errorcode)
{
#if HAVE_GA
    GA_Error(const_cast<char*>(message.c_str()), errorcode);
#elif HAVE_MPI
    cerr << "[" << nodeid() << "] " << message << " :: " << errorcode << endl;
    MPI_Abort(MPI_COMM_WORLD, errorcode);
#else
#   error
#endif
}


/**
 * Returns the number of nodes in this calculation.
 *
 * This abstracts away the chosen messaging library.
 *
 * @return the number of nodes in this calculation
 */
int64_t pagoda::num_nodes()
{
    return ProcessGroup::get_default().get_size();
}


/**
 * Returns the ID of this node in this calculation.
 *
 * This abstracts away the chosen messaging library.
 *
 * @return the ID of this node in this calculation.
 */
int64_t pagoda::nodeid()
{
    return ProcessGroup::get_default().get_rank();
}
