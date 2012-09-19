/**
 * Perform multidimensional array transposition using iterators.
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
    double  *src_buf = NULL;
    double  *src_ptr = NULL;
    int64_t  src_ndim = -1;
    int64_t  src_nd_m1 = -1;
    int64_t  src_elems_prod = 1;
    vector<int64_t> src_elems;
    vector<int64_t> src_coords;
    vector<int64_t> src_dims_m1;
    vector<int64_t> src_strides;
    vector<int64_t> src_backstrides;

    double  *dst_buf = NULL;
    double  *dst_ptr = NULL;;
    int64_t  dst_ndim = -1;
    int64_t  dst_nd_m1 = -1;
    int64_t  dst_elems_prod = 1;
    vector<int64_t> dst_elems;
    vector<int64_t> dst_elems_abs;
    vector<bool>    dst_reverse;
    vector<int64_t> dst_coords;
    vector<int64_t> dst_dims_m1;
    vector<int64_t> dst_strides;
    vector<int64_t> dst_backstrides;

    vector<int64_t> dim_permute_user;
    vector<int64_t> dim_map;
#if DEBUG
    vector<int64_t> trn_coords;
#endif

    /* parse command line */
    if (argc != 3) {
        cout << "usage: TestTranspose 1,2,3 3,2,1" << endl;
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
            dst_elems.push_back(length);
            dst_elems_abs.push_back(length < 0 ? -length : length);
            dst_reverse.push_back(length < 0);
        }
    }
    if (src_elems.size() != dst_elems.size()) {
        cout << "source and destination shapes must have the same number of elements" << endl;
        return EXIT_FAILURE;
    }

    src_ndim = src_elems.size();
    src_nd_m1 = src_ndim-1;
    src_coords.resize(src_ndim);
    src_dims_m1.resize(src_ndim);
    src_strides.resize(src_ndim);
    src_backstrides.resize(src_ndim);

    dst_ndim = dst_elems.size();
    dst_nd_m1 = dst_ndim-1;
    dst_coords.resize(dst_ndim);
    dst_dims_m1.resize(dst_ndim);
    dst_strides.resize(dst_ndim);
    dst_backstrides.resize(dst_ndim);

    dim_permute_user.resize(src_ndim);
    dim_map.resize(src_ndim);
#if DEBUG
    trn_coords.resize(src_ndim);
#endif

    for (size_t i=0; i<src_ndim; ++i) {
        vector<int64_t>::iterator pos;
        pos = find(dst_elems_abs.begin(), dst_elems_abs.end(), src_elems[i]);
        if (pos == dst_elems_abs.end()) {
            cout << "dst does not related to src" << endl;
            return EXIT_FAILURE;
        }
        dim_map[i] = pos - dst_elems_abs.begin();
    }
    for (size_t i=0; i<src_ndim; ++i ) {
        dim_permute_user[dim_map[i]] = i;
    }

    /* number of source elements */
    src_elems_prod = accumulate(
            src_elems.begin(), src_elems.end(), 1, multiplies<int64_t>());
    src_buf = new double[src_elems_prod];
    src_ptr = src_buf;
    /* fill src_buf with enumeration */
    generate(src_buf, src_buf+src_elems_prod, gen_enum_t());

    /* number of destination elements */
    dst_elems_prod = accumulate(
            dst_elems_abs.begin(), dst_elems_abs.end(), 1, multiplies<int64_t>());
    dst_buf = new double[dst_elems_prod];
    dst_ptr = dst_buf;
    fill(dst_buf, dst_buf+dst_elems_prod, -1);
    assert(src_elems_prod == dst_elems_prod);
#if DEBUG
    cout << STR_ARR(dim_permute_user,src_ndim) << endl;
    cout << STR_ARR(dim_map,src_ndim) << endl;
    cout << STR_ARR(src_elems,src_ndim) << endl;
    cout << STR_ARR(dst_elems,dst_ndim) << endl;
#endif

    /* src iterator setup */
    for (int64_t i=src_nd_m1; i>=0; --i) {
        src_coords[i] = 0;
        src_dims_m1[i] = src_elems[i]-1;
        src_strides[i] = (i == src_nd_m1) ? 1 : src_strides[i+1]*src_elems[i+1];
        src_backstrides[i] = src_dims_m1[i]*src_strides[i];
    }
#if DEBUG
    cout << STR_ARR(src_strides,src_ndim) << endl;
    cout << STR_ARR(src_backstrides,src_ndim) << endl;
#endif

    /* dst iterator setup */
    for (int64_t i=dst_nd_m1; i>=0; --i) {
        dst_coords[i] = 0;
        dst_dims_m1[i] = dst_elems_abs[i]-1;
        dst_strides[i] = (i == dst_nd_m1) ? 1 : dst_strides[i+1]*dst_elems_abs[i+1];
        dst_backstrides[i] = dst_dims_m1[i]*dst_strides[i];
    }
    int64_t dst_buf_movement = 0;
    for (int64_t i=dst_nd_m1; i>=0; --i) {
        if (dst_elems[i] < 0) {
            dst_buf_movement += dst_strides[i]*dst_dims_m1[i];
        }
    }
    cout << "dst_buf_movement=" << dst_buf_movement << endl;
    dst_ptr += dst_buf_movement;
    for (int64_t i=dst_nd_m1; i>=0; --i) {
        if (dst_elems[i] < 0) {
            dst_strides[i] = -dst_strides[i];
            dst_backstrides[i] = -dst_backstrides[i];
        }
    }
#if DEBUG
    cout << STR_ARR(dst_strides,dst_ndim) << endl;
    cout << STR_ARR(dst_backstrides,dst_ndim) << endl;
#endif

    for (int64_t elem=0; elem<src_elems_prod; ++elem) {
#if DEBUG
        cout << elem;
        for (int64_t i=0; i<src_ndim; ++i) {
            cout << " " << src_coords[i];
        }
        cout << "\t-->\t";
        for (int64_t i=0; i<dst_ndim; ++i) {
            if (dst_reverse[i]) {
                cout << " " << (src_dims_m1[dim_map[i]]-src_coords[dim_map[i]]);
            }
            else {
                cout << " " << src_coords[dim_map[i]];
            }
            trn_coords[i] = src_coords[dim_map[i]];
        }
        cout << "\t";
        cout << pagoda::ravel_index(trn_coords, dst_elems_abs) << endl;
#endif
        *dst_ptr = *src_ptr;
        for (int64_t i=src_nd_m1; i>=0; --i) {
            if (src_coords[i] < src_dims_m1[i]) {
                ++src_coords[i];
                src_ptr += src_strides[i];
                dst_ptr += dst_strides[dim_map[i]];
                break;
            }
            else {
                src_coords[i] = 0;
                src_ptr -= src_backstrides[i];
                dst_ptr -= dst_backstrides[dim_map[i]];
            }
        }
    }

    double sum=0;
    for (int64_t i=0; i<dst_elems_prod; ++i) {
        sum += dst_buf[i];
        //if (i%17 == 0) {
        if (i%13 == 0) {
            cout << endl;
        }
        cout << setw(4) << dst_buf[i];
    }
    cout << endl;
    cout << sum << endl;

    delete [] src_buf;
    delete [] dst_buf;

    return EXIT_SUCCESS;
}
