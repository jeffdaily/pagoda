#include <stdint.h>

#include <iostream>
#include <sstream>
#include <vector>

using std::cout;
using std::endl;
using std::istringstream;
using std::vector;

#include "pagoda.H"
#include "timer.h"

#define BYTES_PER_GB 1073741824.0

int main(int argc, char **argv)
{
    int nelem = 500000;
    int start = 0;
    int stop = nelem;
    int step = 1;
    vector<bool> vec_bool;
    vector<int>  vec_int;
    vector<char> vec_char;
    Array *mask = NULL;
    IndexHyperslab slab;
    unsigned long long timer_bool;
    unsigned long long timer_int;
    unsigned long long timer_char;
    unsigned long long timer_mask;
    double bytes_bool;
    double bytes_int;
    double bytes_char;
    double bytes_mask;

    pagoda::initialize(&argc,&argv);
    timer_init();

    if (argc > 1) {
        istringstream arg(argv[1]);
        arg >> nelem;
        stop = nelem;
    }
    if (argc > 2) {
        istringstream arg(argv[2]);
        arg >> start;
    }
    if (argc > 3) {
        istringstream arg(argv[3]);
        arg >> stop;
    }
    if (argc > 4) {
        istringstream arg(argv[4]);
        arg >> step;
    }

    if (0 == pagoda::me) {
        cout << "nelem=" << nelem << endl;
        cout << "start=" << start << endl;
        cout << " stop=" << stop << endl;
        cout << " step=" << step << endl;
    }
    vec_bool.assign(nelem, false);
    vec_int.assign(nelem, 0);
    vec_char.assign(nelem, 0);
    slab = IndexHyperslab("slab", start, stop, step);
    bytes_bool = 1.0 * nelem / 8.0 / BYTES_PER_GB;
    bytes_int = 1.0 * nelem * sizeof(int) / BYTES_PER_GB;
    bytes_char = 1.0 * nelem * sizeof(char) / BYTES_PER_GB;
    bytes_mask = 1.0 * nelem * sizeof(int) / BYTES_PER_GB;

    if (0 == pagoda::me) cout << "starting bool loop" << endl;
    timer_bool = timer_start();
    for (int i=start; i<stop; i+=step) {
        vec_bool[i] = true;
    }
    timer_bool = timer_end(timer_bool);

    if (0 == pagoda::me) cout << "starting int loop" << endl;
    timer_int = timer_start();
    for (int i=start; i<stop; i+=step) {
        vec_int[i] = 1;
    }
    timer_int = timer_end(timer_int);

    if (0 == pagoda::me) cout << "starting char loop" << endl;
    timer_char = timer_start();
    for (int i=start; i<stop; i+=step) {
        vec_char[i] = 1;
    }
    timer_char = timer_end(timer_char);

    // now for the parallel part
    mask = Array::mask_create("mask", nelem);
    if (0 == pagoda::me) cout << "starting mask loop" << endl;
    timer_mask = timer_start();
    mask->modify(slab);
    timer_mask = timer_end(timer_mask);
    delete mask;

    if (0 == pagoda::me) {
        cout << "bytes_bool=" << bytes_bool << endl;
        cout << " bytes_int=" << bytes_int << endl;
        cout << "bytes_char=" << bytes_char << endl;
        cout << "bytes_mask=" << bytes_mask << endl;
        cout << endl;
        cout << "timer_name=" << timer_name() << endl;
        cout << "timer_bool=" << timer_bool << endl;
        cout << " timer_int=" << timer_int << endl;
        cout << "timer_char=" << timer_char << endl;
        cout << "timer_mask=" << timer_mask << endl;
    }

    pagoda::finalize();
    return 0;
}

