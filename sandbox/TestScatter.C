/**
 * @file TestScatter
 *
 * Test Array::scatter() functionality.
 */
#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <vector>
using namespace std;

#include "Array.H"
#include "Bootstrap.H"
#include "Collectives.H"
#include "DataType.H"
#include "Debug.H"
#include "Pack.H"
#include "Util.H"


template <class T> void print1d(Array *array);
template <class T> void print2d(Array *array);


int main(int argc, char **argv)
{
    Array *array;
    Array *index;
    vector<int64_t> shape(2,20);
    vector<int> buffer;
    vector<int64_t> subscripts;
    int enumerate_start = 1;

    pagoda::initialize(&argc, &argv);

    array = Array::create(DataType::INT, shape);
    array->fill_value(pagoda::npe);

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
    pagoda::print_zero("scatter test results\n");
    print2d<int>(array);

    // this tests pagoda::enumerate
    index = Array::create(DataType::INT, vector<int64_t>(1, pagoda::npe+1));
    pagoda::enumerate(index, &enumerate_start, NULL);
    pagoda::print_zero("enumerate test results\n");
    print1d<int>(index);

    // this tests pagoda::renumber
    pagoda::renumber(array, index);
    pagoda::print_zero("renumber test results\n");
    print2d<int>(array);

    delete array;
    delete index;

    pagoda::finalize();

    return EXIT_SUCCESS;
};


template <class T>
void print1d(Array *array)
{
    if (0 == pagoda::me) {
        vector<int64_t> shape = array->get_shape();
        vector<int64_t> lo(1,0);
        vector<int64_t> hi(shape);
        int64_t idx = 0;
        T *data;

        hi[0] -= 1;
        data = (T*)array->get(lo,hi);

        for (int64_t i=0; i<shape[0]; ++i) {
            cout << setw(3) << data[idx++];
        }
        cout << endl;

        delete [] data;
    }
    pagoda::barrier();
}


template <class T>
void print2d(Array *array)
{
    if (0 == pagoda::me) {
        vector<int64_t> shape = array->get_shape();
        vector<int64_t> lo(2,0);
        vector<int64_t> hi(shape);
        int64_t idx = 0;
        T *data;

        hi[0] -= 1;
        hi[1] -= 1;
        data = (T*)array->get(lo,hi);

        for (int64_t i=0; i<shape[0]; ++i) {
            for (int64_t j=0; j<shape[1]; ++j) {
                cout << setw(3) << data[idx++];
            }
            cout << endl;
        }

        delete [] data;
    }
    pagoda::barrier();
}
