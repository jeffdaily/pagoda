#ifndef NUMERIC_H_
#define NUMERIC_H_

#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <functional>
#include <numeric>
#include <iostream>
#include <vector>

#include "Error.H"

using std::accumulate;
using std::binary_function;
using std::bind2nd;
using std::cout;
using std::endl;
using std::greater;
using std::less_equal;
using std::multiplies;
using std::remove_copy_if;
using std::vector;

class DataType;

/**
 * Holds miscellanous functionality.
 */
namespace pagoda
{

    template <class T, class U> void iadd(T& x, const U& y) {
        x+=static_cast<const T>(y);
    };

    template <class T, class U> void isub(T& x, const U& y) {
        x-=static_cast<const T>(y);
    };

    template <class T, class U> void imul(T& x, const U& y) {
        x*=static_cast<T>(y);
    };

    template <class T, class U> void idiv(T& x, const U& y) {
        x/=static_cast<T>(y);
    };

    template <class T, class U> void imax(T& x, const U& y) {
        x = std::max(x,static_cast<T>(y));
    };

    template <class T, class U> void imin(T& x, const U& y) {
        x = std::min(x,static_cast<T>(y));
    };

    void transpose(const DataType &type,
            const void *src, const vector<int64_t> &shape,
                  void *dst, const vector<int64_t> &axes);

    void in_place_broadcast_add(
                  void *dst, const vector<int64_t> &dst_shape, const DataType &dst_type,
            const void *src, const vector<int64_t> &src_shape, const DataType &src_type);

    void in_place_broadcast_mul(
                  void *dst, const vector<int64_t> &dst_shape, const DataType &dst_type,
            const void *src, const vector<int64_t> &src_shape, const DataType &src_type);

    void transpose(const DataType &type,
            const void *src, const vector<int64_t> &shape,
                  void *dst, const vector<int64_t> &axes,
                             const vector<int64_t> &reverse);

    void reduce_sum(const DataType &type,
            const void *src, const vector<int64_t> &src_shape,
                  void *dst, const vector<int64_t> &dst_shape);
    
    void reduce_sum(const DataType &type,
            const void *src, const vector<int64_t> &src_shape,
                  void *dst, const vector<int64_t> &dst_shape,
            const void *msk, const vector<int64_t> &msk_shape);

    void reduce_sum(const DataType &type, const DataType &wgt_type,
            const void *src, const vector<int64_t> &src_shape,
                  void *dst, const vector<int64_t> &dst_shape,
            const void *msk, const vector<int64_t> &msk_shape,
            const void *wgt, const vector<int64_t> &wgt_shape);

    template <typename T> void transpose(
            const T *src, const vector<int64_t> &shape,
                  T *dst, const vector<int64_t> &axes);

    template <typename T> void transpose(
            const T *src, const vector<int64_t> &shape,
                  T *dst, const vector<int64_t> &axes,
                          const vector<int64_t> &reverse);

    template <typename T, typename U, class InPlaceOperator>
        void in_place_broadcast_op(
                  T *dst, const vector<int64_t> &dst_shape,
            const U *src, const vector<int64_t> &src_shape,
                          InPlaceOperator op);

    template <typename T> void reduce_sum(
            const T *src, const vector<int64_t> &src_shape,
                  T *dst, const vector<int64_t> &dst_shape);

    template <typename T, typename U> void reduce_sum(
            const T *src, const vector<int64_t> &src_shape,
                  T *dst, const vector<int64_t> &dst_shape,
            const U *msk, const vector<int64_t> &msk_shape);

    template <typename T, typename U, typename V> void reduce_sum(
            const T *src, const vector<int64_t> &src_shape,
                  T *dst, const vector<int64_t> &dst_shape,
            const U *msk, const vector<int64_t> &msk_shape,
            const V *wgt, const vector<int64_t> &wgt_shape);

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
        transpose(src, shape, dst, axes, vector<int64_t>(axes.size(),1));
    }

    /**
     * Transpose the given array.
     *
     * @param[in] src the source buffer, contiguous
     * @param[in] shape the shape of the source buffer
     * @param[in,out] dst the destination buffer, contiguous
     * @param[in] axes the new order of the axes for the shape
     * @param[in] reverse whether to invert an of the axes (1 or -1)
     */
    template <typename T> void transpose(
            const T *src, const vector<int64_t> &shape,
            T *dst, const vector<int64_t> &axes,
            const vector<int64_t> &reverse)
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
        for (int64_t i=dst_nd_m1; i>=0; --i) {
            ASSERT(reverse[i] == 1 || reverse[i] == -1);
            if (reverse[i] == -1) {
                dst += dst_strides[i]*dst_dims_m1[i];
                dst_strides[i] = -dst_strides[i];
                dst_backstrides[i] = -dst_backstrides[i];
            }
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
     * Apply the in-place operator to 'dst' and 'src' arrays where 'src' has
     * fewer dimensions but will be broadcast to fit based on the 'src_shape'.
     *
     * @param[in,out] dst the destination buffer, contiguous
     * @param[in] dst_shape the shape of the destination buffer
     * @param[in,out] src the source buffer, contiguous
     * @param[in] src_shape dimensions to broadcast are indicated by <=0
     * @param[in] op a binary in-place operator e.g. a += b
     */
    template <typename T, typename U, class InPlaceOperator>
        void in_place_broadcast_op(
                  T *dst, const vector<int64_t> &dst_shape,
            const U *src, const vector<int64_t> &src_shape,
                          InPlaceOperator op)
    {
        T *dst_ptr;
        int64_t dst_ndim = dst_shape.size();
        int64_t dst_nd_m1 = dst_ndim-1;
        int64_t dst_elems_prod = 1;
        vector<int64_t> dst_elems = dst_shape;
        vector<int64_t> dst_coords(dst_ndim);
        vector<int64_t> dst_dims_m1(dst_ndim);
        vector<int64_t> dst_strides(dst_ndim);
        vector<int64_t> dst_backstrides(dst_ndim);

        const U *src_ptr;
        int64_t src_ndim = count_if(src_shape.begin(), src_shape.end(),
                                    bind2nd(greater<int64_t>(),0));
        int64_t src_nd_m1 = src_ndim-1;
        int64_t src_elems_prod = 1;
        vector<int64_t> src_elems(src_ndim);
        vector<int64_t> src_coords(src_ndim);
        vector<int64_t> src_dims_m1(src_ndim);
        vector<int64_t> src_strides(src_ndim);
        vector<int64_t> src_backstrides(src_ndim);

        vector<int64_t> brd_strides(dst_ndim);
        vector<int64_t> brd_backstrides(dst_ndim);

        /* remove the invalid values from src_shape */
        remove_copy_if(src_shape.begin(), src_shape.end(), src_elems.begin(),
                bind2nd(less_equal<int64_t>(),0));

        /* dst shape */
        dst_elems_prod = accumulate(
                dst_elems.begin(), dst_elems.end(), 1, multiplies<int64_t>());

        /* src shape */ 
        src_elems_prod = accumulate(
                src_elems.begin(), src_elems.end(), 1, multiplies<int64_t>());

        /* dst iterator setup */
        for (int64_t i=dst_nd_m1; i>=0; --i) {
            dst_coords[i] = 0;
            dst_dims_m1[i] = dst_elems[i]-1;
            dst_strides[i] = (i == dst_nd_m1) ? 1 : dst_strides[i+1]*dst_elems[i+1];
            dst_backstrides[i] = dst_dims_m1[i]*dst_strides[i];
        }

        /* src iterator setup */
        for (int64_t i=src_nd_m1; i>=0; --i) {
            src_coords[i] = 0;
            src_dims_m1[i] = src_elems[i]-1;
            src_strides[i] = (i == src_nd_m1) ? 1 : src_strides[i+1]*src_elems[i+1];
            src_backstrides[i] = src_dims_m1[i]*src_strides[i];
        }

        /* copy src strides into brd (broadcast) strides as appropriate */
        for (int64_t i=0,j=0; i<dst_ndim; ++i) {
            if (src_shape[i] <= 0) {
                brd_strides[i] = 0;
                brd_backstrides[i] = 0;
            }
            else {
                brd_strides[i] = src_strides[j];
                brd_backstrides[i] = src_backstrides[j++];
            }
        }

        dst_ptr = dst;
        src_ptr = src;
        for (int64_t elem=0; elem<dst_elems_prod; ++elem) {
#if DEBUG
            cout << elem;
            for (int64_t i=0; i<dst_ndim; ++i) {
                cout << " " << dst_coords[i];
            }
            cout << endl;
#endif
            op(*dst_ptr, *src_ptr);
            for (int64_t i=dst_nd_m1; i>=0; --i) {
                if (dst_coords[i] < dst_dims_m1[i]) {
                    ++dst_coords[i];
                    dst_ptr += dst_strides[i];
                    src_ptr += brd_strides[i];
                    break;
                }
                else {
                    dst_coords[i] = 0;
                    dst_ptr -= dst_backstrides[i];
                    src_ptr -= brd_backstrides[i];
                }
            }
        }

#if DEBUG
        for (int64_t i=0; i<src_elems[0]; ++i) {
            for (int64_t j=0; j<src_elems[1]; ++j) {
                cout << src[i*src_elems[1] + j] << " ";
            }
            cout << endl;
        }
#endif
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

    /**
     * Reduce the given array using addition with a mask.
     *
     * The destination buffer is filled with the result, regardless of the
     * original values within the dst buffer i.e. the dst buffer is filled with
     * zeros before performing the reduction.
     *
     * @param[in] src the source buffer, contiguous
     * @param[in] src_shape the shape of the source buffer
     * @param[in,out] dst the destination buffer, contiguous
     * @param[in] dst_shape dimensions to reduce are indicated by <=0
     * @param[in] msk the source buffer, contiguous
     * @param[in] msk_shape the shape of the source buffer
     */
    template <typename T, typename U> void reduce_sum(
            const T *src, const vector<int64_t> &src_shape,
                  T *dst, const vector<int64_t> &dst_shape,
            const U *msk, const vector<int64_t> &msk_shape)
    {
        const T *src_ptr;
        int64_t  src_ndim = src_shape.size();
        int64_t  src_nd_m1 = src_ndim-1;
        int64_t  src_elems_prod = 1;
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

        const U *msk_ptr;
        int64_t  msk_ndim = count_if(msk_shape.begin(), msk_shape.end(),
                                     bind2nd(greater<int64_t>(),0));
        int64_t  msk_nd_m1 = msk_ndim-1;
        int64_t  msk_elems_prod = 1;
        vector<int64_t> msk_elems(msk_ndim);
        vector<int64_t> msk_coords(msk_ndim);
        vector<int64_t> msk_dims_m1(msk_ndim);
        vector<int64_t> msk_strides(msk_ndim);
        vector<int64_t> msk_backstrides(msk_ndim);

        vector<int64_t> dst_brd_strides(src_ndim);
        vector<int64_t> dst_brd_backstrides(src_ndim);
        vector<int64_t> msk_brd_strides(src_ndim);
        vector<int64_t> msk_brd_backstrides(src_ndim);

        /* remove the invalid values from dst_shape */
        remove_copy_if(dst_shape.begin(), dst_shape.end(), dst_elems.begin(),
                bind2nd(less_equal<int64_t>(),0));

        /* remove the invalid values from msk_shape */
        remove_copy_if(msk_shape.begin(), msk_shape.end(), msk_elems.begin(),
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

        /* msk shape */ 
        msk_elems_prod = accumulate(
                msk_elems.begin(), msk_elems.end(), 1, multiplies<int64_t>());

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

        /* msk iterator setup */
        for (int64_t i=msk_nd_m1; i>=0; --i) {
            msk_coords[i] = 0;
            msk_dims_m1[i] = msk_elems[i]-1;
            msk_strides[i] = (i == msk_nd_m1) ? 1 : msk_strides[i+1]*msk_elems[i+1];
            msk_backstrides[i] = msk_dims_m1[i]*msk_strides[i];
        }

        /* copy dst strides into brd (broadcast) strides as appropriate */
        for (int64_t i=0,j=0; i<src_ndim; ++i) {
            if (dst_shape[i] <= 0) {
                dst_brd_strides[i] = 0;
                dst_brd_backstrides[i] = 0;
            }
            else {
                dst_brd_strides[i] = dst_strides[j];
                dst_brd_backstrides[i] = dst_backstrides[j++];
            }
        }

        /* copy msk strides into brd (broadcast) strides as appropriate */
        for (int64_t i=0,j=0; i<src_ndim; ++i) {
            if (msk_shape[i] <= 0) {
                msk_brd_strides[i] = 0;
                msk_brd_backstrides[i] = 0;
            }
            else {
                msk_brd_strides[i] = msk_strides[j];
                msk_brd_backstrides[i] = msk_backstrides[j++];
            }
        }

        src_ptr = src;
        dst_ptr = dst;
        msk_ptr = msk;
        for (int64_t elem=0; elem<src_elems_prod; ++elem) {
#if DEBUG
            cout << elem;
            for (int64_t i=0; i<src_ndim; ++i) {
                cout << " " << src_coords[i];
            }
            cout << endl;
#endif
            *dst_ptr += *src_ptr * *msk_ptr;
            for (int64_t i=src_nd_m1; i>=0; --i) {
                if (src_coords[i] < src_dims_m1[i]) {
                    ++src_coords[i];
                    src_ptr += src_strides[i];
                    dst_ptr += dst_brd_strides[i];
                    msk_ptr += msk_brd_strides[i];
                    break;
                }
                else {
                    src_coords[i] = 0;
                    src_ptr -= src_backstrides[i];
                    dst_ptr -= dst_brd_backstrides[i];
                    msk_ptr -= msk_brd_backstrides[i];
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


    /**
     * Reduce the given array using addition with a mask and weight.
     *
     * The destination buffer is filled with the result, regardless of the
     * original values within the dst buffer i.e. the dst buffer is filled with
     * zeros before performing the reduction.
     *
     * @param[in] src the source buffer, contiguous
     * @param[in] src_shape the shape of the source buffer
     * @param[in,out] dst the destination buffer, contiguous
     * @param[in] dst_shape dimensions to reduce are indicated by <=0
     * @param[in] msk the source buffer, contiguous
     * @param[in] msk_shape the shape of the source buffer
     * @param[in] wgt the source buffer, contiguous
     * @param[in] wgt_shape the shape of the source buffer
     */
    template <typename T, typename U, typename V> void reduce_sum(
            const T *src, const vector<int64_t> &src_shape,
                  T *dst, const vector<int64_t> &dst_shape,
            const U *msk, const vector<int64_t> &msk_shape,
            const V *wgt, const vector<int64_t> &wgt_shape)
    {
        const T *src_ptr;
        int64_t  src_ndim = src_shape.size();
        int64_t  src_nd_m1 = src_ndim-1;
        int64_t  src_elems_prod = 1;
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

        const U *msk_ptr;
        int64_t  msk_ndim = count_if(msk_shape.begin(), msk_shape.end(),
                                     bind2nd(greater<int64_t>(),0));
        int64_t  msk_nd_m1 = msk_ndim-1;
        int64_t  msk_elems_prod = 1;
        vector<int64_t> msk_elems(msk_ndim);
        vector<int64_t> msk_coords(msk_ndim);
        vector<int64_t> msk_dims_m1(msk_ndim);
        vector<int64_t> msk_strides(msk_ndim);
        vector<int64_t> msk_backstrides(msk_ndim);

        const V *wgt_ptr;
        int64_t  wgt_ndim = count_if(wgt_shape.begin(), wgt_shape.end(),
                                     bind2nd(greater<int64_t>(),0));
        int64_t  wgt_nd_m1 = wgt_ndim-1;
        int64_t  wgt_elems_prod = 1;
        vector<int64_t> wgt_elems(wgt_ndim);
        vector<int64_t> wgt_coords(wgt_ndim);
        vector<int64_t> wgt_dims_m1(wgt_ndim);
        vector<int64_t> wgt_strides(wgt_ndim);
        vector<int64_t> wgt_backstrides(wgt_ndim);

        vector<int64_t> dst_brd_strides(src_ndim);
        vector<int64_t> dst_brd_backstrides(src_ndim);
        vector<int64_t> msk_brd_strides(src_ndim);
        vector<int64_t> msk_brd_backstrides(src_ndim);
        vector<int64_t> wgt_brd_strides(src_ndim);
        vector<int64_t> wgt_brd_backstrides(src_ndim);

        /* remove the invalid values from dst_shape */
        remove_copy_if(dst_shape.begin(), dst_shape.end(), dst_elems.begin(),
                bind2nd(less_equal<int64_t>(),0));

        /* remove the invalid values from msk_shape */
        remove_copy_if(msk_shape.begin(), msk_shape.end(), msk_elems.begin(),
                bind2nd(less_equal<int64_t>(),0));

        /* remove the invalid values from wgt_shape */
        remove_copy_if(wgt_shape.begin(), wgt_shape.end(), wgt_elems.begin(),
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

        /* msk shape */ 
        msk_elems_prod = accumulate(
                msk_elems.begin(), msk_elems.end(), 1, multiplies<int64_t>());

        /* wgt shape */ 
        wgt_elems_prod = accumulate(
                wgt_elems.begin(), wgt_elems.end(), 1, multiplies<int64_t>());

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

        /* msk iterator setup */
        for (int64_t i=msk_nd_m1; i>=0; --i) {
            msk_coords[i] = 0;
            msk_dims_m1[i] = msk_elems[i]-1;
            msk_strides[i] = (i == msk_nd_m1) ? 1 : msk_strides[i+1]*msk_elems[i+1];
            msk_backstrides[i] = msk_dims_m1[i]*msk_strides[i];
        }

        /* wgt iterator setup */
        for (int64_t i=wgt_nd_m1; i>=0; --i) {
            wgt_coords[i] = 0;
            wgt_dims_m1[i] = wgt_elems[i]-1;
            wgt_strides[i] = (i == wgt_nd_m1) ? 1 : wgt_strides[i+1]*wgt_elems[i+1];
            wgt_backstrides[i] = wgt_dims_m1[i]*wgt_strides[i];
        }

        /* copy dst strides into brd (broadcast) strides as appropriate */
        for (int64_t i=0,j=0; i<src_ndim; ++i) {
            if (dst_shape[i] <= 0) {
                dst_brd_strides[i] = 0;
                dst_brd_backstrides[i] = 0;
            }
            else {
                dst_brd_strides[i] = dst_strides[j];
                dst_brd_backstrides[i] = dst_backstrides[j++];
            }
        }

        /* copy msk strides into brd (broadcast) strides as appropriate */
        for (int64_t i=0,j=0; i<src_ndim; ++i) {
            if (msk_shape[i] <= 0) {
                msk_brd_strides[i] = 0;
                msk_brd_backstrides[i] = 0;
            }
            else {
                msk_brd_strides[i] = msk_strides[j];
                msk_brd_backstrides[i] = msk_backstrides[j++];
            }
        }

        /* copy wgt strides into brd (broadcast) strides as appropriate */
        for (int64_t i=0,j=0; i<src_ndim; ++i) {
            if (wgt_shape[i] <= 0) {
                wgt_brd_strides[i] = 0;
                wgt_brd_backstrides[i] = 0;
            }
            else {
                wgt_brd_strides[i] = wgt_strides[j];
                wgt_brd_backstrides[i] = wgt_backstrides[j++];
            }
        }

        src_ptr = src;
        dst_ptr = dst;
        msk_ptr = msk;
        wgt_ptr = wgt;
        for (int64_t elem=0; elem<src_elems_prod; ++elem) {
#if DEBUG
            cout << elem;
            for (int64_t i=0; i<src_ndim; ++i) {
                cout << " " << src_coords[i];
            }
            cout << endl;
#endif
            *dst_ptr += static_cast<T>(*src_ptr * *msk_ptr * *wgt_ptr);
            for (int64_t i=src_nd_m1; i>=0; --i) {
                if (src_coords[i] < src_dims_m1[i]) {
                    ++src_coords[i];
                    src_ptr += src_strides[i];
                    dst_ptr += dst_brd_strides[i];
                    msk_ptr += msk_brd_strides[i];
                    wgt_ptr += wgt_brd_strides[i];
                    break;
                }
                else {
                    src_coords[i] = 0;
                    src_ptr -= src_backstrides[i];
                    dst_ptr -= dst_brd_backstrides[i];
                    msk_ptr -= msk_brd_backstrides[i];
                    wgt_ptr -= wgt_brd_backstrides[i];
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
