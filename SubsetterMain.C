#include <mpi.h>

#include <algorithm>
    using std::copy;
#include <iostream>
    using std::cout;
    using std::endl;
#include <iterator>
    using std::ostream_iterator;

#include "AggregationUnion.H"
#include "Dataset.H"
#include "Dimension.H"
#include "LatLonBox.H"
#include "Mask.H"
#include "NetcdfDataset.H"
#include "NetcdfFileWriter.H"
#include "Slice.H"
#include "SubsetterCommands.H"
#include "Util.H"


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();

    SubsetterCommands cmd;
    Dataset *dataset;
    Aggregation *agg;

    try {
        cmd.parse(argc,argv);
    }
    catch (SubsetterException &ex) {
        if (ME == 0)
            cout << ex.what() << endl;
        exit(EXIT_FAILURE);
    }

    const vector<string> &infiles = cmd.get_intput_filenames();
    dataset = agg = new AggregationUnion;
    for (size_t i=0,limit=infiles.size(); i<limit; ++i) {
        agg->add(Dataset::open(infiles[i]));
    }

    Util::calculate_required_memory(dataset->get_vars());

    dataset->adjust_masks(cmd.get_slices());
    dataset->adjust_masks(cmd.get_box());

    NetcdfFileWriter::write(cmd.get_output_filename(), dataset);

    delete dataset;
    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
}

