/**
 * Perform multidimensional Array transposition using Array::transpose().
 *
 * Transpose is determined based on command line arguments.
 * For example, the usual coplete transpose is 
 *      TestTranspose 1,2,3 3,2,1
 * But we could transpose arbitrarily
 *      TestTranspose 1,2,3 2,3,1
 *
 * The transpose is determined based on matching between the two command line
 * parameters rather than by giving the array shape followed by the newly
 * specified order in terms of axis indices.
 */
#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

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

#include "Array.H"
#include "Bootstrap.H"
#include "Collectives.H"
#include "DataType.H"
#include "Debug.H"
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
    Array *g_src = NULL;
    int  *src_buf = NULL;
    int64_t src_ndim = -1;
    vector<int64_t> src_elems;

    Array *g_dst = NULL;
    int  *dst_buf = NULL;
    vector<int64_t> dst_elems;
    vector<int64_t> dst_reverse;

    vector<int64_t> dim_permute_user;
    vector<int64_t> dim_map;

    pagoda::initialize(&argc, &argv);

    /* parse command line */
    if (argc != 3) {
        if (0 == pagoda::me) {
            cout << "usage: TestTranspose 1,2,3 3,2,1" << endl;
        }
        pagoda::finalize();
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
            src_elems.push_back(length);
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
            if (length < 0) {
                dst_elems.push_back(-length);
                dst_reverse.push_back(-1);
            }
            else {
                dst_elems.push_back(length);
                dst_reverse.push_back(1);
            }
        }
    }
    if (src_elems.size() != dst_elems.size()) {
        if (0 == pagoda::me) {
            cout << "source and destination shapes must have the same number of elements" << endl;
        }
        pagoda::finalize();
        return EXIT_FAILURE;
    }

    src_ndim = src_elems.size();
    dim_permute_user.resize(src_ndim);
    dim_map.resize(src_ndim);

    for (size_t i=0; i<src_ndim; ++i) {
        vector<int64_t>::iterator pos;
        pos = find(dst_elems.begin(), dst_elems.end(), src_elems[i]);
        if (pos == dst_elems.end()) {
            if (0 == pagoda::me) {
                cout << "dst does not related to src" << endl;
            }
            pagoda::finalize();
            return EXIT_FAILURE;
        }
        dim_map[i] = pos - dst_elems.begin();
    }
    for (size_t i=0; i<src_ndim; ++i ) {
        dim_permute_user[dim_map[i]] = i;
    }

    /* create the source Array and fill with enumeration */
    DataType type = DataType::INT;
    g_src = Array::create(type, src_elems);
    if (0 == pagoda::me) {
        int64_t src_elems_prod = g_src->get_size();
        src_buf = new int[src_elems_prod];
        /* fill src_buf with enumeration */
        generate(src_buf, src_buf+src_elems_prod, gen_enum_t());
        g_src->put(src_buf);
        delete [] src_buf;
    }
    pagoda::barrier();

    //g_dst = g_src->transpose(dim_permute_user, dst_reverse);
    g_dst = g_src->transpose(dim_permute_user);
    assert(NULL != g_dst);

    if (0 == pagoda::me) {
        dst_buf = static_cast<int*>(g_dst->get());
        double sum=0;
        for (int64_t i=0,limit=g_dst->get_size(); i<limit; ++i) {
            sum += dst_buf[i];
            //if (i%17 == 0) {
            if (i%13 == 0) {
                cout << endl;
            }
            cout << setw(4) << dst_buf[i];
        }
        cout << endl;
        cout << sum << endl;

        delete [] dst_buf;
    }
#if DEBUG
    if (0 == pagoda::me) {
        cout << STR_ARR(dim_permute_user,src_ndim) << endl;
        cout << STR_ARR(dim_map,src_ndim) << endl;
        cout << STR_ARR(src_elems,src_ndim) << endl;
        cout << STR_ARR(dst_elems,src_ndim) << endl;
        cout << STR_ARR(g_src->get_shape(),src_ndim) << endl;
        cout << STR_ARR(g_dst->get_shape(),src_ndim) << endl;
    }
#endif

    delete g_src;
    delete g_dst;

    pagoda::finalize();
    return EXIT_SUCCESS;
}
