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

    /* CommandLineParser interprets the command line.
     * For example, -bN,S,E,W for a lat/lon box
     * as well as an input files and the single output file.  */ 
    CommandLineParser cmd(argc,argv);

    /* The Dataset constructor knows what to do with the CommandLineParser
     * and figures out inputs, outputs, etc. to construct the Dataset.
     * Alternatively, the dataset could be created explicitly
     * e.g.
     * Dataset dataset(input_filename);
     * Dataset dataset(cmd.get_input_filenames()) */
    Dataset *dataset = new Dataset(cmd);

    /* The above Dataset constructor also implicitly defined the subset,
     * if any subset arguments had appeared on the command line.
     * Alternatively, the subset could be defined explicitly
     * e.g.
     * datset->define_subset(cmd.get_latlonbox());
     * datset->define_subset(cmd.get_slices()); // start,stop,step for dims */

    /* We are ready to create the output file.
     * The correct output file format is selected based on the given
     * filename's extension e.g. *.nc for (Parallel) NetCDF.  */ 
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
