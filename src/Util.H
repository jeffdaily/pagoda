#ifndef UTIL_H_
#define UTIL_H_

#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

using std::divides;
using std::modulus;
using std::multiplies;
using std::ostringstream;
using std::partial_sum;
using std::ptr_fun;
using std::reverse;
using std::rotate;
using std::string;
using std::transform;
using std::vector;

class Variable;

/**
 * Holds miscellanous functionality.
 */
namespace pagoda
{
    int64_t num_nodes();
    int64_t nodeid();
    void barrier();
    void abort(const string &message);
    void abort(const string &message, int errorcode);
    void gop_sum(vector<char> &values);
    void gop_sum(vector<short> &values);
    void gop_sum(vector<int> &values);
    void gop_sum(vector<long> &values);
    void gop_sum(vector<long long> &values);
#if NEED_VECTOR_INT64_T_GOP
    void gop_sum(vector<int64_t> &values);
#endif
    void gop_sum(vector<float> &values);
    void gop_sum(vector<double> &values);
    void gop_sum(vector<long double> &values);
    void gop_sum(vector<unsigned char> &values);
    void gop_sum(vector<unsigned short> &values);
    void gop_sum(vector<unsigned int> &values);
    void gop_sum(vector<unsigned long> &values);
    void gop_sum(vector<unsigned long long> &values);
    void gop_sum(char &value);
    void gop_sum(short &value);
    void gop_sum(int &value);
    void gop_sum(long &value);
    void gop_sum(long long &value);
#if NEED_VECTOR_INT64_T_GOP
    void gop_sum(int64_t &value);
#endif
    void gop_sum(float &value);
    void gop_sum(double &value);
    void gop_sum(long double &value);
    void gop_sum(unsigned char &value);
    void gop_sum(unsigned short &value);
    void gop_sum(unsigned int &value);
    void gop_sum(unsigned long &value);
    void gop_sum(unsigned long long &value);

    void gop_min(vector<char> &values);
    void gop_min(vector<short> &values);
    void gop_min(vector<int> &values);
    void gop_min(vector<long> &values);
    void gop_min(vector<long long> &values);
#if NEED_VECTOR_INT64_T_GOP
    void gop_min(vector<int64_t> &values);
#endif
    void gop_min(vector<float> &values);
    void gop_min(vector<double> &values);
    void gop_min(vector<long double> &values);
    void gop_min(vector<unsigned char> &values);
    void gop_min(vector<unsigned short> &values);
    void gop_min(vector<unsigned int> &values);
    void gop_min(vector<unsigned long> &values);
    void gop_min(vector<unsigned long long> &values);
    void gop_min(char &value);
    void gop_min(short &value);
    void gop_min(int &value);
    void gop_min(long &value);
    void gop_min(long long &value);
#if NEED_VECTOR_INT64_T_GOP
    void gop_min(int64_t &value);
#endif
    void gop_min(float &value);
    void gop_min(double &value);
    void gop_min(long double &value);
    void gop_min(unsigned char &value);
    void gop_min(unsigned short &value);
    void gop_min(unsigned int &value);
    void gop_min(unsigned long &value);
    void gop_min(unsigned long long &value);

    void gop_max(vector<char> &values);
    void gop_max(vector<short> &values);
    void gop_max(vector<int> &values);
    void gop_max(vector<long> &values);
    void gop_max(vector<long long> &values);
#if NEED_VECTOR_INT64_T_GOP
    void gop_max(vector<int64_t> &values);
#endif
    void gop_max(vector<float> &values);
    void gop_max(vector<double> &values);
    void gop_max(vector<long double> &values);
    void gop_max(vector<unsigned char> &values);
    void gop_max(vector<unsigned short> &values);
    void gop_max(vector<unsigned int> &values);
    void gop_max(vector<unsigned long> &values);
    void gop_max(vector<unsigned long long> &values);
    void gop_max(char &value);
    void gop_max(short &value);
    void gop_max(int &value);
    void gop_max(long &value);
    void gop_max(long long &value);
#if NEED_VECTOR_INT64_T_GOP
    void gop_max(int64_t &value);
#endif
    void gop_max(float &value);
    void gop_max(double &value);
    void gop_max(long double &value);
    void gop_max(unsigned char &value);
    void gop_max(unsigned short &value);
    void gop_max(unsigned int &value);
    void gop_max(unsigned long &value);
    void gop_max(unsigned long long &value);

    void broadcast(vector<char> &values, int root);
    void broadcast(vector<short> &values, int root);
    void broadcast(vector<int> &values, int root);
    void broadcast(vector<long> &values, int root);
    void broadcast(vector<long long> &values, int root);
    void broadcast(vector<float> &values, int root);
    void broadcast(vector<double> &values, int root);
    void broadcast(vector<long double> &values, int root);
    void broadcast(vector<unsigned char> &values, int root);
    void broadcast(vector<unsigned short> &values, int root);
    void broadcast(vector<unsigned int> &values, int root);
    void broadcast(vector<unsigned long> &values, int root);
    void broadcast(vector<unsigned long long> &values, int root);
    void broadcast(char &value, int root);
    void broadcast(short &value, int root);
    void broadcast(int &value, int root);
    void broadcast(long &value, int root);
    void broadcast(long long &value, int root);
    void broadcast(float &value, int root);
    void broadcast(double &value, int root);
    void broadcast(long double &value, int root);
    void broadcast(unsigned char &value, int root);
    void broadcast(unsigned short &value, int root);
    void broadcast(unsigned int &value, int root);
    void broadcast(unsigned long &value, int root);
    void broadcast(unsigned long long &value, int root);

    vector<int64_t> get_shape(const vector<int64_t> &lo, const vector<int64_t> &hi);
    int64_t shape_to_size(const vector<int64_t> &shape);
    bool starts_with(const string &str, const string &start);
    bool ends_with(const string &str, const string &end);
    vector<string> split(const string &s);
    vector<string> split(const string &s, char delimiter);
    void trim(string &s);
    void calculate_required_memory();
    void calculate_required_memory(const vector<Variable*> &vars);
    template <class T> T ptr_deleter(T object);
    template <class T> bool same_name(const T *first, const T *second);
    template <class T> vector<T> unravel_index(T x, const vector<T> &dims);
    template <class T> T ravel_index(vector<T> indices, const vector<T> &dims);
    template <class T> string to_string(const T &value);
    bool file_exists(const string &filename);
    void print_backtrace();
    template <typename T> void transpose(
            const T *src, const vector<int64_t> &shape,
            T *dst, const vector<int64_t> &axes);

    /**
     * Generic unary function for calling "delete" on the given object.
     *
     * The class T is assumed to be a pointer.
     *
     * @param[in] object the pointer to delete
     * @return NULL
     */
    template <class T>
    T ptr_deleter(T object)
    {
        delete object;
        return NULL;
    }


    /**
     * Generic binary test for equality of the "get_name()" accessor.
     * The class T is assumed to be an interface defining "get_name()".
     *
     * @param[in] first the lhs of equality
     * @param[in] second theh rhs of equality
     */
    template <class T>
    bool same_name(const T *first, const T *second)
    {
        return first->get_name() == second->get_name();
    }


    /**
     * Given a 1-D index, determine its equivalent N-D index based on dims.
     *
     * Adapted from Python's NumPy.
     *
     * @param[in] x the 1-D index
     * @param[in] dims the shape of the N-D array
     * @return the N-D index
     */
    template <class T>
    vector<T> unravel_index(T x, const vector<T> &dims)
    {
        vector<T> result(dims.rbegin(), dims.rend());
        // [d,c,b,a]
        rotate(result.begin(), result.end()-1, result.end());
        // [a,d,c,b]
        *(result.begin()) = 1;
        // [1,d,c,b]
        partial_sum(result.begin(), result.end(), result.begin(), multiplies<T>());
        // [1,d,dc,dcb]
        reverse(result.begin(), result.end());
        // [dcb,dc,d,1]
        transform(result.begin(), result.end(), result.begin(),
                  bind1st(divides<T>(),x));
        // [x/dcb, x/dc, x/d, x/1]
        transform(result.begin(), result.end(), dims.begin(), result.begin(),
                  modulus<T>());
        // [x/dcb % a, x/dc % b, x/d % c, x/1 % d]
        return result;
    }


    /**
     * Given an N-D index, determine its equivalent 1-D index based on dims.
     *
     * Adapted from Python's NumPy.
     *
     * @param[in] indices the N-D index
     * @param[in] dims the shape of the N-D array
     * @return the 1-D index
     */
    template <class T>
    T ravel_index(vector<T> indices, const vector<T> &dims)
    {
        // indices [w,x,y,z], dims [a,b,c,d]
        vector<T> offsets(dims.begin()+1, dims.end());
        // [b,c,d]
        reverse(offsets.begin(), offsets.end());
        // [d,c,b]
        partial_sum(offsets.begin(), offsets.end(), offsets.begin(),
                    multiplies<T>());
        // [d,cd,bcd]
        reverse(offsets.begin(), offsets.end());
        // [bcd,cd,d]
        transform(indices.begin(), indices.end()-1, offsets.begin(),
                  indices.begin(), multiplies<T>());
        // [wd,xcd,ybcd,z]
        return accumulate(indices.begin(), indices.end()-1, 0) + indices.back();
    }


    /**
     * Conveniently return stringified object using streams.
     *
     * @param[in] value what we are converting to a string
     * @return the stringified object
     */
    template <class T> string to_string(const T &value)
    {
        ostringstream os;
        os << value;
        return os.str();
    }

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
} // namespace pagoda

#endif // UTIL_H_
