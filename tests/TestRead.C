/** Perform various block-sized pnetcdf reads. */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>

#include "Array.H"
#include "Bootstrap.H"
#include "Dataset.H"
#include "Variable.H"

using std::cerr;
using std::cout;
using std::endl;


/**
 * 1) Open a dataset.
 * 2) Attempt to read variable into an Array.
 * 3) Dump the values.
 */
int main(int argc, char **argv)
{
    Dataset *dataset;
    Variable *variable;
    Array *array;

    pagoda::initialize(&argc, &argv);

    if (3 != argc) {
        if (0 == pagoda::me) {
            cerr << "Usage: TestRead filename variablename" << endl;
        }
        pagoda::finalize();
        return EXIT_FAILURE;
    }

    dataset = Dataset::open(argv[1]);
    variable = dataset->find_var(argv[2]);
    array = variable->read(int64_t(0));
    array->dump();

    delete dataset;

    pagoda::finalize();

    return EXIT_SUCCESS;
}
