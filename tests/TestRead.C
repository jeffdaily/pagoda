/** Perform various block-sized pnetcdf reads. */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>

#include "Array.H"
#include "Bootstrap.H"
#include "Dataset.H"
#include "Debug.H"
#include "Variable.H"

using std::cerr;
using std::cout;
using std::endl;


/**
 * 1) Open a dataset.
 * 2) Attempt to read variable into an Array.
 * 3) Compare the blocking and non-blocking values.
 */
int main(int argc, char **argv)
{
    Dataset *dataset;
    Variable *variable;
    Array *array_blocking;
    Array *array_nonblocking;
    bool ok = true;

    pagoda::initialize(&argc, &argv);

    if (3 != argc) {
        if (0 == pagoda::me) {
            cerr << "Usage: TestRead filename variablename" << endl;
        }
        pagoda::finalize();
        return EXIT_FAILURE;
    }

    dataset = Dataset::open(argv[1]);
    variable = dataset->get_var(argv[2]);

    // blocking
    if (0 == pagoda::me) {
        cerr << "blocking read... ";
    }
    array_blocking = variable->read(int64_t(0));
    if (0 == pagoda::me) {
        cerr << "done" << endl;
    }

    // non-blocking
    if (0 == pagoda::me) {
        cerr << "non-blocking read... ";
    }
    array_nonblocking = variable->iread(int64_t(0));
    if (0 == pagoda::me) {
        cerr << "done" << endl;
    }
    if (0 == pagoda::me) {
        cerr << "wait... ";
    }
    dataset->wait();
    if (0 == pagoda::me) {
        cerr << "done" << endl;
    }

    // compare the arrays
    // it is assumed they have the same distributions
    if (array_blocking->owns_data()) {
        DataType type = array_blocking->get_type();
        int64_t size = array_blocking->get_local_size();
#define compare_helper(DT,T) \
        if (type == DT) { \
            T *b  = (T*)array_blocking->access(); \
            T *nb = (T*)array_nonblocking->access(); \
            for (int64_t i=0; i<size; ++i) { \
                ok &= (b[i] == nb[i]); \
                if (!ok) { \
                    break; \
                } \
            } \
        } else
        compare_helper(DataType::INT, int)
        compare_helper(DataType::FLOAT, float)
        compare_helper(DataType::DOUBLE, double)
        {
            if (0 == pagoda::me) {
                cerr << "unhandled data type" << endl;
                ok = false;
            }
        }
#undef compare_helper
    }

    if (ok) {
        PRINT_SYNC("ok\n");
    } else {
        PRINT_SYNC("failure\n");
    }

    delete dataset;
    delete array_blocking;
    delete array_nonblocking;

    pagoda::finalize();

    return EXIT_SUCCESS;
}
