#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif // HAVE_CONFIG_H

// C++ includes, std and otherwise
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// C++ includes
#include "Array.H"
#include "Bootstrap.H"
#include "Collectives.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Util.H"

using std::cerr;
using std::cout;
using std::endl;
using std::ostringstream;
using std::string;
using std::vector;


int main(int argc, char **argv)
{
    Dataset *dataset = NULL;
    Array *mask = NULL;
    vector<Dimension*> dims;

    pagoda::initialize(&argc,&argv);

    if (argc != 2) {
        cerr << "Usage: TestDimension <filename>" << endl;
        return EXIT_FAILURE;
    }

    dataset = Dataset::open(argv[1]);

    dims = dataset->get_dims();
    for (size_t dimid=0,limit=dims.size(); dimid<limit; ++dimid) {
        Dimension *dim = dims[dimid];

        cout << dim << endl;
        if (dim->get_name() == "cells") {
            mask = Array::mask_create(dim);
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

