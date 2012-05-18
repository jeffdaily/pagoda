#ifndef NUMERIC_H_
#define NUMERIC_H_

#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <functional>
#include <numeric>
#include <vector>

using std::accumulate;
using std::bind2nd;
using std::greater;
using std::less_equal;
using std::multiplies;
using std::remove_copy_if;
using std::vector;


/**
 * Holds miscellanous functionality.
 */
namespace pagoda
{
    template <typename T> void transpose(
            const T *src, const vector<int64_t> &shape,
                  T *dst, const vector<int64_t> &axes);
    template <typename T> void reduce_sum(
            const T *src, const vector<int64_t> &src_shape,
                  T *dst, const vector<int64_t> &dst_shape);

    /**
     * Transpose the given array.
     *
     * @param[in] src the source buffer, contiguous
     * @param[in] shape the shape of the source buffer
     * @param[in,out] dst the destination buffer, contiguous
     * @param[in] axes the new order of the axes for the shape
     */
    template <typename T> void transpose(
            const T *src, const vector<int64_t> &shape,
            T *dst, const vector<int64_t> &axes)
    {
        int64_t src_ndim = shape.size();
        int64_t src_nd_m1 = src_ndim-1;
        int64_t src_elems_prod = 1;
        vector<int64_t> src_elems = shape;
        vector<int64_t> src_coords(shape.size());
        vector<int64_t> src_dims_m1(shape.size());
        vector<int64_t> src_strides(shape.size());
        vector<int64_t> src_backstrides(shape.size());

        int64_t dst_ndim = shape.size();
        int64_t dst_nd_m1 = dst_ndim-1;
        int64_t dst_elems_prod = 1;
        vector<int64_t> dst_elems(shape.size());
        vector<int64_t> dst_coords(shape.size());
        vector<int64_t> dst_dims_m1(shape.size());
        vector<int64_t> dst_strides(shape.size());
        vector<int64_t> dst_backstrides(shape.size());

        vector<int64_t> dim_permute_user = axes;
        vector<int64_t> dim_map(shape.size());

        assert(axes.size() == shape.size());
        for (int64_t i=0,limit=src_elems.size(); i<limit; ++i) {
            dim_map.at(dim_permute_user[i]) = i;
            dst_elems[i] = src_elems.at(dim_permute_user[i]);
        }

        /* number of source elements */
        src_elems_prod = accumulate(
                src_elems.begin(), src_elems.end(), 1, multiplies<int64_t>());

        /* number of destination elements */
        dst_elems_prod = accumulate(
                dst_elems.begin(), dst_elems.end(), 1, multiplies<int64_t>());
        assert(src_elems_prod == dst_elems_prod);

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
            dst_backstrides[i] = dst_dims_m1[i]*dst_strides[i];
        }

        for (int64_t elem=0; elem<src_elems_prod; ++elem) {
            *dst = *src;
            for (int64_t i=src_nd_m1; i>=0; --i) {
                if (src_coords[i] < src_dims_m1[i]) {
                    ++src_coords[i];
                    src += src_strides[i];
                    dst += dst_strides[dim_map[i]];
                    break;
                }
                else {
                    src_coords[i] = 0;
                    src -= src_backstrides[i];
                    dst -= dst_backstrides[dim_map[i]];
                }
            }
        }
    }

    /**
     * Reduce the given array using addition.
     *
     * The destination buffer is filled with the result, regardless of the
     * original values within the dst buffer i.e. the dst buffer is filled with
     * zeros before performing the reduction.
     *
     * @param[in] src the source buffer, contiguous
     * @param[in] src_shape the shape of the source buffer
     * @param[in,out] dst the destination buffer, contiguous
     * @param[in] dst_shape dimensions to reduce are indicated by <=0
     */
    template <typename T> void reduce_sum(
            const T *src, const vector<int64_t> &src_shape,
            T *dst, const vector<int64_t> &dst_shape)
    {
        const T *src_ptr;
        int64_t src_ndim = src_shape.size();
        int64_t src_nd_m1 = src_ndim-1;
        int64_t src_elems_prod = 1;
        vector<int64_t> src_elems = src_shape;
        vector<int64_t> src_coords(src_ndim);
        vector<int64_t> src_dims_m1(src_ndim);
        vector<int64_t> src_strides(src_ndim);
        vector<int64_t> src_backstrides(src_ndim);

        T *dst_ptr;
        int64_t dst_ndim = count_if(dst_shape.begin(), dst_shape.end(),
                                    bind2nd(greater<int64_t>(),0));
        int64_t dst_nd_m1 = dst_ndim-1;
        int64_t dst_elems_prod = 1;
        vector<int64_t> dst_elems(dst_ndim);
        vector<int64_t> dst_coords(dst_ndim);
        vector<int64_t> dst_dims_m1(dst_ndim);
        vector<int64_t> dst_strides(dst_ndim);
        vector<int64_t> dst_backstrides(dst_ndim);

        vector<int64_t> brd_strides(src_ndim);
        vector<int64_t> brd_backstrides(src_ndim);

        /* remove the invalid values from dst_shape */
        remove_copy_if(dst_shape.begin(), dst_shape.end(), dst_elems.begin(),
                bind2nd(less_equal<int64_t>(),0));

        /* source shape */
        src_elems_prod = accumulate(
                src_elems.begin(), src_elems.end(), 1, multiplies<int64_t>());

        /* dst shape */ 
        dst_elems_prod = accumulate(
                dst_elems.begin(), dst_elems.end(), 1, multiplies<int64_t>());
        /* fill dst with 0 */
        for (int i=0; i<dst_elems_prod; ++i) {
            dst[i] = 0;
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

        /* copy dst strides into brd (broadcast) strides as appropriate */
        for (int64_t i=0,j=0; i<src_ndim; ++i) {
            if (dst_shape[i] <= 0) {
                brd_strides[i] = 0;
                brd_backstrides[i] = 0;
            }
            else {
                brd_strides[i] = dst_strides[j];
                brd_backstrides[i] = dst_backstrides[j++];
            }
        }

        src_ptr = src;
        dst_ptr = dst;
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

#if DEBUG
        for (int64_t i=0; i<dst_elems[0]; ++i) {
            for (int64_t j=0; j<dst_elems[1]; ++j) {
                cout << dst[i*dst_elems[1] + j] << " ";
            }
            cout << endl;
        }
#endif
    }

} // namespace pagoda

#endif // NUMERIC_H_
