#include <ga.h>
#include <mpi.h>

#include "CommandLineParser.H"
#include "Dataset.H"
#include "FileWriter.H"

int main(int argc, char **argv)
{
    // The usual initialization.
    MPI_Init(&argc, &argv);
    GA_Initialize();

    CommandLineParser cmd(argc,argv);
    Dataset *dataset = new Dataset(cmd);
    Variable *ts = dataset->get_var("ts");

    /* calculate zonal average
     * It will act appropriately for regular or geodesic grids.
     * So for ts(time,level,cells) we get result(time,level,lat)
     */
    int num_bins = 180;
    Variable *zave = ts->dim_bin(num_bins);

    FileWriter *writer = FileWriter::create(cmd.get_output_filename());
    /* The simplest way to define the output is to pass in an input dataset.
     * The prior subset established with the input dataset will be consulted
     * when defining the output dataset.  For instance, if the dimension
     * grid_cells was reduced, the reduced size will be defined in the output
     * dataset. All Dimensions and Variables from the given dataset will be
     * defined in the output dataset (unless the omission of one or more was
     * established as part of the subset e.g. -v VarName on the command line */
    writer->define(dataset);

    /* The write function is explicit since many underlying file formats
     * constrain file definition and file writing to separate phases for
     * performance reasons.
     * Alternatively, individual variables could be written
     * e.g.
     * vector<Variable*>::iterator var=dataset->get_vars.begin();
     * vector<Variable*>::iterator end=dataset->get_vars.end();
     * for (; var!=end; ++var) {
     *     writer->write(*var);
     * } */
    writer->write(dataset);

    delete writer; /* also closes the output file */
    delete dataset; /* closes any open input files */

    /* The usual termination. */
    GA_Terminate();
    MPI_Finalize();
    return 0;
}
