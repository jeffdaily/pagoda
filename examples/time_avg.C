#include <mpi.h>
#include <ga.h>

#include "CommandLineParser.H"
#include "Dataset.H"
#include "FileWriter.H"
#include "Variable.H"

int main(int argc, char **argv)
{
    // The usual initialization.
    MPI_Init(&argc, &argv);
    GA_Initialize();

    CommandLineParser cmd(argc,argv);
    Dataset *dataset = new Dataset(cmd);

    // ts(time,level,cells)
    Variable *ts = dataset->get_var("ts");

    Variable *notime  = ts->dim_avg(0); // average away the 0th dimension
    Variable *three   = ts->time_avg(3); // average together every 3 timesteps
    Variable *monthly = ts->monthly_average();
    Variable *weekly  = ts->weekly_average();

    FileWriter *writer = FileWriter::create("monthly.nc");
    writer->define(monthly);
    writer->write(monthly);

    delete writer; /* also closes the output file */
    delete dataset; /* closes any open input files */

    /* The usual termination. */
    GA_Terminate();
    MPI_Finalize();
    return 0;
}
