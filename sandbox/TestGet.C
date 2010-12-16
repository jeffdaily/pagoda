/**
 * @file TestGet
 *
 * Test Array::get() functionality.
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
#include "Debug.H"
#include "Util.H"


int main(int argc, char **argv)
{
    vector<int64_t> data_shape;
    vector<int64_t> data_local_shape;
    Array *array;
    int *data = NULL;

    vector<int64_t> buf_shape;
    int64_t buf_size;
    int *buf = NULL;

    vector<int64_t> data_lo(2,0);
    vector<int64_t> data_hi;
    vector<int64_t> lo;
    vector<int64_t> hi;
    vector<int64_t> alo;
    vector<int64_t> ahi;

    pagoda::initialize(&argc, &argv);

    data_shape.push_back(15);
    data_shape.push_back(10);
    array = Array::create(DataType::INT, data_shape);
    data_local_shape = array->get_local_shape();
    array->get_distribution(alo,ahi);

    buf_shape.push_back(5);
    buf_shape.push_back(10);
    buf_size = pagoda::shape_to_size(buf_shape);
    buf = new int[buf_size];

    lo.push_back(10);
    lo.push_back(0);
    hi.push_back(14);
    hi.push_back(5);

    if (array->owns_data()) {
        data = (int*)array->access();
        for (int i=0,ilimit=data_local_shape[0]; i<ilimit; ++i) {
            for (int j=0,jlimit=data_local_shape[1]; j<jlimit; ++j) {
                data[i*jlimit + j] = (i+alo[0])*10+j+alo[1];
            }
        }
        array->release_update();
    }

    if (0 == pagoda::me) {
        cout << "array" << endl;
        data_hi.push_back(data_shape[0]-1);
        data_hi.push_back(data_shape[1]-1);
        data = (int*)array->get(data_lo,data_hi);
        for (int i=0,ilimit=data_shape[0]; i<ilimit; ++i) {
            for (int j=0,jlimit=data_shape[1]; j<jlimit; ++j) {
                cout << setw(5) << data[i*data_shape[1] + j];
            }
            cout << endl;
        }
        delete [] data;
    }
    
    for (int i=0; i<buf_size; ++i) {
        buf[i] = i+1000;
    }

    if (0 == pagoda::me) {
        cout << "buffer before get" << endl;
        for (int i=0; i<buf_shape[0]; ++i) {
            for (int j=0; j<buf_shape[1]; ++j) {
                cout << setw(5) << buf[i*buf_shape[1] + j];
            }
            cout << endl;
        }
    }

    buf = (int*)array->get(lo, hi, buf);

    if (0 == pagoda::me) {
        cout << "buffer after get" << endl;
        for (int i=0; i<buf_shape[0]; ++i) {
            for (int j=0; j<buf_shape[1]; ++j) {
                cout << setw(5) << buf[i*buf_shape[1] + j];
            }
            cout << endl;
        }
    }

    delete [] buf;

    pagoda::finalize();

    return EXIT_SUCCESS;
};
