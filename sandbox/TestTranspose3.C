/**
 * Perform multidimensional array transposition using pagoda::transpose().
 *
 * Transpose is determined based on command line arguments.
 * For example, the usual coplete transpose is 
 *      TestTranspose 3,4,5 2,1,0
 * But we could transpose arbitrarily
 *      TestTranspose 3,4,5 2,0,1
 *
 * The transpose is determined based on the given array shape follows by the
 * newly specified axis order in terms of axis indices.
 */
#include <cassert>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

using std::accumulate;
using std::cout;
using std::endl;
using std::fill;
using std::generate;
using std::istringstream;
using std::multiplies;
using std::setw;
using std::string;
using std::vector;

#include "Print.H"
#include "Numeric.H"
#include "Util.H"

#define DEBUG 1
#if DEBUG
#   define STR_ARR(vec,n) pagoda::arr_to_string(vec,n,",",#vec)
#endif
// class generator:
typedef struct gen_enum {
  int current;
  gen_enum(int _current=0) {current=_current;}
  int operator()() {return current++;}
} gen_enum_t;

int main(int argc, char **argv)
{
    double *src = NULL;
    double *dst = NULL;
    int64_t nelem = 1;
    vector<int64_t> shape;
    vector<int64_t> axes;

    /* parse command line */
    if (argc != 3) {
        cout << "usage:   TestTranspose3 SHAPE AXES" << endl;
        cout << "example: TestTranspose3 1,2,3 0,2,1" << endl;
        return EXIT_FAILURE;
    }
    else {
        istringstream iss(argv[1]);

        while (!iss.eof()) {
            string token;
            istringstream token_as_stream;
            int64_t length;

            getline(iss, token, ',');
            token_as_stream.str(token);
            token_as_stream >> length;
            shape.push_back(length);
        }

        iss.clear();
        iss.str(argv[2]);
        while (!iss.eof()) {
            string token;
            istringstream token_as_stream;
            int64_t length;

            getline(iss, token, ',');
            token_as_stream.str(token);
            token_as_stream >> length;
            axes.push_back(length);
        }
    }
    if (shape.size() != axes.size()) {
        cout << "shape and axes must have the same number of elements" << endl;
        return EXIT_FAILURE;
    }

    /* number of elements */
    nelem = accumulate(shape.begin(), shape.end(), 1, multiplies<int64_t>());
    src = new double[nelem];
    /* fill src with enumeration */
    generate(src, src+nelem, gen_enum_t());

    dst = new double[nelem];
    fill(dst, dst+nelem, -1);

    pagoda::transpose(src, shape, dst, axes);

    double sum=0;
    for (int64_t i=0; i<nelem; ++i) {
        sum += dst[i];
        if (i%17 == 0) {
            cout << endl;
        }
        cout << setw(4) << dst[i];
    }
    cout << endl;
    cout << sum << endl;

    delete [] src;
    delete [] dst;

    return EXIT_SUCCESS;
}
