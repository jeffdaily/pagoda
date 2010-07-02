#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif // HAVE_CONFIG_H

// C++ includes, std and otherwise
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using std::cerr;
using std::cout;
using std::endl;
using std::ostringstream;
using std::string;
using std::vector;

// C++ includes
#include "Bootstrap.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Mask.H"
#include "SubsetterException.H"
#include "Util.H"


int main(int argc, char **argv)
{
    Dataset *dataset = NULL;
    Mask *mask = NULL;
    vector<Dimension*> dims;

    pagoda::initialize(&argc,&argv);

    if (argc != 2) {
        cerr << "Usage: TestDimension <filename>" << endl;
        return EXIT_FAILURE;
    }

    dataset = Dataset::open(argv[1]);

    pagoda::calculate_required_memory(dataset->get_vars());

    dims = dataset->get_dims();
    for (size_t dimid=0,limit=dims.size(); dimid<limit; ++dimid) {
        Dimension *dim = dims[dimid];

        cout << dim << endl;
        if (dim->get_name() == "cells") {
            mask = Mask::create(dim);
        }
    }

    if (mask) {
        Array *array = mask->reindex();
        pagoda::barrier();
        mask->dump();
        array->dump();
    }

    delete mask;
    delete dataset;

    pagoda::finalize();

    return EXIT_SUCCESS;
}

