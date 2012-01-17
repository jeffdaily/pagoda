/**
 * Find a way to perform multidimensional array transposition using iterators.
 */
#include <functional>
#include <iomanip>
#include <iostream>
#include <numeric>

#include "Print.H"
#include "Util.H"

using std::accumulate;
using std::cout;
using std::endl;
using std::multiplies;
using std::setw;

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
    double  *src_buf;
    double  *src_ptr;
    int64_t  src_ndim = 4;
    int64_t  src_nd_m1 = src_ndim-1;
    int64_t  src_elems_prod = 1;
    vector<int64_t> src_elems(src_ndim);
    vector<int64_t> src_coords(src_ndim);
    vector<int64_t> src_dims_m1(src_ndim);
    vector<int64_t> src_strides(src_ndim);
    vector<int64_t> src_backstrides(src_ndim);

    double  *dst_buf;
    double  *dst_ptr;
    int64_t  dst_ndim = 4;
    int64_t  dst_nd_m1 = dst_ndim-1;
    int64_t  dst_elems_prod = 1;
    vector<int64_t> dst_elems(dst_ndim);
    vector<int64_t> dst_coords(dst_ndim);
    vector<int64_t> dst_dims_m1(dst_ndim);
    vector<int64_t> dst_strides(dst_ndim);
    vector<int64_t> dst_backstrides(dst_ndim);

    vector<int64_t> dim_permute_user(src_ndim);
    vector<int64_t> dim_map(src_ndim);
#if DEBUG
    vector<int64_t> trn_coords(src_ndim);
#endif

    dim_permute_user[0] = 0;
    dim_permute_user[1] = 3;
    dim_permute_user[2] = 1;
    dim_permute_user[3] = 2;
    dim_map[dim_permute_user[0]] = 0;
    dim_map[dim_permute_user[1]] = 1;
    dim_map[dim_permute_user[2]] = 2;
    dim_map[dim_permute_user[3]] = 3;

    /* source shape */
    src_elems[0] = 2;
    src_elems[1] = 3;
    src_elems[2] = 4;
    src_elems[3] = 5;
    src_elems_prod = accumulate(
            src_elems.begin(), src_elems.end(), 1, multiplies<int64_t>());
    src_buf = new double[src_elems_prod];
    /* fill src_buf with enumeration */
    std::generate(src_buf, src_buf+src_elems_prod, gen_enum_t());

    /* dst shape, transposing as given above in dim_map */
    dst_elems[dim_map[0]] = src_elems[0];
    dst_elems[dim_map[1]] = src_elems[1];
    dst_elems[dim_map[2]] = src_elems[2];
    dst_elems[dim_map[3]] = src_elems[3];
    dst_elems_prod = accumulate(
            dst_elems.begin(), dst_elems.end(), 1, multiplies<int64_t>());
    dst_buf = new double[dst_elems_prod];
    std::fill(dst_buf, dst_buf+dst_elems_prod, -1);
#if DEBUG
    pagoda::println_sync(STR_ARR(dim_permute_user,src_ndim));
    pagoda::println_sync(STR_ARR(dim_map,src_ndim));
    pagoda::println_sync(STR_ARR(src_elems,src_ndim));
    pagoda::println_sync(STR_ARR(dst_elems,dst_ndim));
#endif

    /* src iterator setup */
    for (int64_t i=src_nd_m1; i>=0; --i) {
        src_coords[i] = 0;
        src_dims_m1[i] = src_elems[i]-1;
        src_strides[i] = (i == src_nd_m1) ? 1 : src_strides[i+1]*src_elems[i+1];
        src_backstrides[i] = src_dims_m1[i]*src_strides[i];
    }
#if DEBUG
    pagoda::println_sync(STR_ARR(src_strides,src_ndim));
    pagoda::println_sync(STR_ARR(src_backstrides,src_ndim));
#endif

    /* dst iterator setup */
    for (int64_t i=dst_nd_m1; i>=0; --i) {
        dst_coords[i] = 0;
        dst_dims_m1[i] = dst_elems[i]-1;
        dst_strides[i] = (i == dst_nd_m1) ? 1 : dst_strides[i+1]*dst_elems[i+1];
        dst_strides[i] = (i == dst_nd_m1) ? 1 : dst_strides[i+1]*dst_elems[i+1];
        dst_backstrides[i] = dst_dims_m1[i]*dst_strides[i];
    }
#if DEBUG
    pagoda::println_sync(STR_ARR(dst_strides,dst_ndim));
    pagoda::println_sync(STR_ARR(dst_backstrides,dst_ndim));
#endif

    src_ptr = src_buf;
    dst_ptr = dst_buf;
    for (int64_t elem=0; elem<src_elems_prod; ++elem) {
#if DEBUG
        cout << elem;
        for (int64_t i=0; i<src_ndim; ++i) {
            cout << " " << src_coords[i];
        }
        cout << "\t-->\t";
        for (int64_t i=0; i<dst_ndim; ++i) {
            cout << " " << src_coords[dim_map[i]];
            trn_coords[i] = src_coords[dim_map[i]];
        }
        cout << "\t";
        cout << pagoda::ravel_index(trn_coords, dst_elems) << endl;
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
        if (i%13 == 0) {
            cout << endl;
        }
        cout << setw(4) << dst_buf[i] << ',';
    }
    cout << endl;
    cout << sum << endl;

    delete [] src_buf;
    delete [] dst_buf;
}
