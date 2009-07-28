#ifdef HAVE_CONFIG_H
#include "config.h"
#endif // HAVE_CONFIG_H

// C includes, std and otherwise
#include <ga.h>
#include <macdecls.h>
#include <mpi.h>
#include <pnetcdf.h>
#include <unistd.h>

// C++ includes, std and otherwise
#include <iostream>
using std::cout;
using std::endl;
#include <sstream>
using std::ostringstream;
#include <string>
using std::string;
#include <vector>
using std::vector;

// C++ includes
#include "Dimension.H"
#include "DistributedMask.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "SubsetterException.H"
#include "Util.H"


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();

    if (argc < 2) return EXIT_FAILURE;

    for (int argi=0; argi<argc; ++argi) {
        cout << argv[argi] << endl;
    }

    NetcdfDataset *dataset = new NetcdfDataset(argv[1]);
    DistributedMask *mask = NULL;

    Util::calculate_required_memory(dataset->get_vars());

    vector<Dimension*> dims = dataset->get_dims();
    for (size_t dimid=0,ndim=dims.size(); dimid<ndim; ++dimid) {
        Dimension *dim = dims[dimid];
        cout << dim << endl;
        if (dim->get_name() == "cells") {
            mask = new DistributedMask(dim, 1);
        }
    }

    if (mask) {
        mask->reindex();
        //GA_Print(mask->get_handle_index());
    }

    delete mask;
    delete dataset;

    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return EXIT_SUCCESS;
}

