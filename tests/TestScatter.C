/**
 * @file TestScatter
 *
 * Test Array::scatter() functionality.
 */
#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cstdlib>
#include <iomanip>
#include <iostream>
using namespace std;

#include "Array.H"
#include "Bootstrap.H"
#include "DataType.H"
#include "Debug.H"
#include "Util.H"


int main(int argc, char **argv)
{
    Array *array;
    vector<int64_t> shape(2,20);
    vector<double> buffer;
    vector<int64_t> subscripts;
    double NEG_ONE = -1;

    pagoda::initialize(&argc, &argv);
    pagoda::calculate_required_memory();

    array = Array::create(DataType::DOUBLE, shape);
    array->fill(&NEG_ONE);

    // each process scatters their id
    for (int64_t i=pagoda::me; i<shape[1]; i+=pagoda::npe) {
        subscripts.push_back(i);
        subscripts.push_back(i);
        buffer.push_back(pagoda::me);
    }
    for (int64_t i=0; i<shape[1]; i+=3) {
        subscripts.push_back(pagoda::me);
        subscripts.push_back(i);
        buffer.push_back(pagoda::me);
        if (i+1 < shape[1] && pagoda::me+pagoda::npe+1 < shape[1]) {
            subscripts.push_back(pagoda::me+pagoda::npe+1);
            subscripts.push_back(i+1);
            buffer.push_back(pagoda::me);
        }
    }
    array->scatter(&buffer[0], subscripts);
    pagoda::barrier();

    if (0 == pagoda::me) {
        vector<int64_t> lo(2,0);
        vector<int64_t> hi(shape);
        int64_t idx = 0;
        double *data;

        hi[0] -= 1;
        hi[1] -= 1;
        data = (double*)array->get(lo,hi);

        for (int64_t i=0; i<shape[0]; ++i) {
            for (int64_t j=0; j<shape[1]; ++j) {
                cout << setw(3) << data[idx++];
            }
            cout << endl;
        }

        delete [] data;
    }

    delete array;

    pagoda::finalize();

    return EXIT_SUCCESS;
};
