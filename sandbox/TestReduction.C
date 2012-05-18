/**
 * Find a way to perform multidimensional array reductions using iterators.
 */
#include <iostream>
#include "Numeric.H"

using std::cout;
using std::endl;

void test1()
{
    double  *src_buf;
    double  *src_ptr;
    int64_t  src_ndim = 4;
    int64_t  src_nd_m1 = src_ndim-1;
    int64_t  src_elems_prod = 1;
    int64_t *src_elems = new int64_t[src_ndim];
    int64_t *src_coords = new int64_t[src_ndim];
    int64_t *src_dims_m1 = new int64_t[src_ndim];
    int64_t *src_strides = new int64_t[src_ndim];
    int64_t *src_backstrides = new int64_t[src_ndim];

    double  *dst_buf;
    double  *dst_ptr;
    int64_t  dst_ndim = 2;
    int64_t  dst_nd_m1 = dst_ndim-1;
    int64_t  dst_elems_prod = 1;
    int64_t *dst_elems = new int64_t[dst_ndim];
    int64_t *dst_coords = new int64_t[dst_ndim];
    int64_t *dst_dims_m1 = new int64_t[dst_ndim];
    int64_t *dst_strides = new int64_t[dst_ndim];
    int64_t *dst_backstrides = new int64_t[dst_ndim];

    int64_t *brd_strides = new int64_t[src_ndim];
    int64_t *brd_backstrides = new int64_t[src_ndim];

    /* source shape */
    src_elems[0] = 3;
    src_elems[1] = 4;
    src_elems[2] = 5;
    src_elems[3] = 6;
    src_elems_prod = src_elems[0]*src_elems[1]*src_elems[2]*src_elems[3];
    src_buf = new double[src_elems_prod];
    /* fill src_buf with enumeration */
    for (int i=0; i<src_elems_prod; ++i) {
        src_buf[i] = i;
        //cout << src_buf[i] << " ";
        //if (i % 25 == 0) cout << endl;
    }

    /* dst shape (we will reduce two of the dimensions */
    dst_elems[0] = 4;
    dst_elems[1] = 5;
    dst_elems_prod = dst_elems[0]*dst_elems[1];
    dst_buf = new double[dst_elems_prod];
    /* fill dst_buf with 0 */
    for (int i=0; i<dst_elems_prod; ++i) {
        dst_buf[i] = 0;
    }

    /* src iterator setup */
    for (int64_t i=src_nd_m1; i>=0; --i) {
        src_coords[i] = 0;
        src_dims_m1[i] = src_elems[i]-1;
        src_strides[i] = (i == src_nd_m1) ? 1 : src_strides[i+1]*src_elems[i+1];
        src_backstrides[i] = src_dims_m1[i]*src_strides[i];
    }

    /* dst iterator setup */
    for (int64_t i=dst_nd_m1; i>=0; --i) {
        dst_coords[i] = 0;
        dst_dims_m1[i] = dst_elems[i]-1;
        dst_strides[i] = (i == dst_nd_m1) ? 1 : dst_strides[i+1]*dst_elems[i+1];
        dst_strides[i] = (i == dst_nd_m1) ? 1 : dst_strides[i+1]*dst_elems[i+1];
        dst_backstrides[i] = dst_dims_m1[i]*dst_strides[i];
    }

    /* for the dst strides we set the reduced dims to 0 */
    //brd_strides[0] = dst_strides[0];
    //brd_strides[1] = 0;
    brd_strides[0] = 0;
    brd_strides[1] = dst_strides[0];
    brd_strides[2] = dst_strides[1];
    brd_strides[3] = 0;
    //brd_backstrides[0] = dst_backstrides[0];
    //brd_backstrides[1] = 0;
    brd_backstrides[0] = 0;
    brd_backstrides[1] = dst_backstrides[0];
    brd_backstrides[2] = dst_backstrides[1];
    brd_backstrides[3] = 0;

    src_ptr = src_buf;
    dst_ptr = dst_buf;
    for (int64_t elem=0; elem<src_elems_prod; ++elem) {
#if DEBUG
        cout << elem;
        for (int64_t i=0; i<src_ndim; ++i) {
            cout << " " << src_coords[i];
        }
        cout << endl;
#endif
        *dst_ptr += *src_ptr;
        for (int64_t i=src_nd_m1; i>=0; --i) {
            if (src_coords[i] < src_dims_m1[i]) {
                ++src_coords[i];
                src_ptr += src_strides[i];
                dst_ptr += brd_strides[i];
                break;
            }
            else {
                src_coords[i] = 0;
                src_ptr -= src_backstrides[i];
                dst_ptr -= brd_backstrides[i];
            }
        }
    }

    for (int64_t i=0; i<dst_elems[0]; ++i) {
        for (int64_t j=0; j<dst_elems[1]; ++j) {
            cout << dst_buf[i*dst_elems[1] + j] << " ";
        }
        cout << endl;
    }
    cout << endl;

    delete [] src_elems;
    delete [] src_coords;
    delete [] src_dims_m1;
    delete [] src_strides;
    delete [] src_backstrides;
    delete [] dst_elems;
    delete [] dst_coords;
    delete [] dst_dims_m1;
    delete [] dst_strides;
    delete [] dst_backstrides;
    delete [] brd_strides;
    delete [] brd_backstrides;
    delete [] src_buf;
    delete [] dst_buf;
}

void test2()
{
    /* source shape */
    vector<int64_t> src_shape;
    src_shape.push_back(3);
    src_shape.push_back(4);
    src_shape.push_back(5);
    src_shape.push_back(6);
    vector<int64_t> dst_shape = src_shape;
    dst_shape[0] = 0;
    dst_shape[3] = 0;
    vector<int64_t> dst_shape_nonzero;
    for (int64_t i=0; i<dst_shape.size(); ++i) {
        if (dst_shape[i] > 0) {
            dst_shape_nonzero.push_back(dst_shape[i]);
        }
    }
    int64_t src_nelems = std::accumulate(
            src_shape.begin(), src_shape.end(), 1, std::multiplies<int64_t>());
    int64_t dst_nelems = std::accumulate(
            dst_shape_nonzero.begin(), dst_shape_nonzero.end(),
            1, std::multiplies<int64_t>());
    double *src_buf = new double[src_nelems];
    double *dst_buf = new double[dst_nelems];
    /* fill src_buf with enumeration */
    for (int i=0; i<src_nelems; ++i) {
        src_buf[i] = i;
    }
    /* fill dst_buf with 0 */
    for (int i=0; i<dst_nelems; ++i) {
        dst_buf[i] = 0;
    }

    pagoda::reduce_sum(src_buf, src_shape, dst_buf, dst_shape);

    for (int64_t i=0; i<dst_shape_nonzero[0]; ++i) {
        for (int64_t j=0; j<dst_shape_nonzero[1]; ++j) {
            cout << dst_buf[i*dst_shape_nonzero[1] + j] << " ";
        }
        cout << endl;
    }
    cout << endl;

    delete [] src_buf;
    delete [] dst_buf;

}

void test3()
{
    /* source shape */
    vector<int64_t> src_shape;
    src_shape.push_back(3);
    src_shape.push_back(4);
    src_shape.push_back(5);
    src_shape.push_back(6);
    vector<int64_t> dst_shape = src_shape;
    dst_shape[0] = 0;
    dst_shape[1] = 0;
    dst_shape[3] = 0;
    vector<int64_t> dst_shape_nonzero;
    for (int64_t i=0; i<dst_shape.size(); ++i) {
        if (dst_shape[i] > 0) {
            dst_shape_nonzero.push_back(dst_shape[i]);
        }
    }
    int64_t src_nelems = std::accumulate(
            src_shape.begin(), src_shape.end(), 1, std::multiplies<int64_t>());
    int64_t dst_nelems = std::accumulate(
            dst_shape_nonzero.begin(), dst_shape_nonzero.end(),
            1, std::multiplies<int64_t>());
    double *src_buf = new double[src_nelems];
    double *dst_buf = new double[dst_nelems];
    /* fill src_buf with enumeration */
    for (int i=0; i<src_nelems; ++i) {
        src_buf[i] = i;
    }
    /* fill dst_buf with 0 */
    for (int i=0; i<dst_nelems; ++i) {
        dst_buf[i] = 0;
    }

    pagoda::reduce_sum(src_buf, src_shape, dst_buf, dst_shape);

    for (int64_t i=0; i<dst_shape_nonzero[0]; ++i) {
        cout << dst_buf[i] << " ";
    }
    cout << endl;
    cout << endl;

    delete [] src_buf;
    delete [] dst_buf;

}

void test4()
{
    /* source shape */
    vector<int64_t> src_shape;
    src_shape.push_back(3);
    src_shape.push_back(4);
    src_shape.push_back(5);
    src_shape.push_back(6);
    vector<int64_t> dst_shape = src_shape;
    dst_shape[0] = 0;
    dst_shape[1] = 0;
    dst_shape[2] = 0;
    dst_shape[3] = 0;
    vector<int64_t> dst_shape_nonzero;
    for (int64_t i=0; i<dst_shape.size(); ++i) {
        if (dst_shape[i] > 0) {
            dst_shape_nonzero.push_back(dst_shape[i]);
        }
    }
    int64_t src_nelems = std::accumulate(
            src_shape.begin(), src_shape.end(), 1, std::multiplies<int64_t>());
    int64_t dst_nelems = std::accumulate(
            dst_shape_nonzero.begin(), dst_shape_nonzero.end(),
            1, std::multiplies<int64_t>());
    double *src_buf = new double[src_nelems];
    double *dst_buf = new double[dst_nelems];
    /* fill src_buf with enumeration */
    for (int i=0; i<src_nelems; ++i) {
        src_buf[i] = i;
    }
    /* fill dst_buf with 0 */
    for (int i=0; i<dst_nelems; ++i) {
        dst_buf[i] = 0;
    }

    pagoda::reduce_sum(src_buf, src_shape, dst_buf, dst_shape);

    cout << dst_buf[0] << " ";
    cout << endl;
    cout << endl;

    delete [] src_buf;
    delete [] dst_buf;

}

int main(int argc, char **argv)
{
    test1();
    test2();
    test3();
    test4();
}
