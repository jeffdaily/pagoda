#ifndef UTIL_H_
#define UTIL_H_

#include <stdint.h>

#include <algorithm>
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
    void abort(const char *message);
    void abort(const char *message, int errorcode);
    void gop_min(vector<long> &values);
    void gop_sum(vector<int> &values);
    void gop_sum(vector<long> &values);
    void gop_sum(vector<long long> &values);
#if NEED_VECTOR_INT64_T_GOP
    void gop_sum(vector<int64_t> &values);
#endif
    void gop_sum(vector<float> &values);
    void gop_sum(vector<double> &values);
    void gop_sum(vector<long double> &values);
    void gop_sum(int &value);
    void gop_sum(long &value);
    void gop_sum(long long &value);
#if NEED_VECTOR_INT64_T_GOP
    void gop_sum(int64_t &value);
#endif
    void gop_sum(float &value);
    void gop_sum(double &value);
    void gop_sum(long double &value);
    vector<int64_t> get_shape(const vector<int64_t> &lo, const vector<int64_t> &hi);
    int64_t shape_to_size(const vector<int64_t> &shape);
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

} // namespace pagoda

#endif // UTIL_H_
