#ifndef ARRAY_H_
#define ARRAY_H_

#include <stdint.h>

#include <vector>

using std::vector;

#include "DataType.H"
#include "Dimension.H"
#include "Error.H"


/**
 * N-dimensional distributed Array interface.
 *
 * Some of the methods of Array assume the data is distributed.  Each process
 * may "own" a portion of the Array's data.  Data is assumed to be distributed
 * in some form of cartesian distribution.  When accessed, the local data can
 * be manipulated as a 1D C array.  This is how new operations may be written.
 */
class Array
{
    public:

        /**
         * Factory Array creation taking a shape directly.
         *
         * @param[in] type of Array to create
         * @param[in] shape of the Array to create
         * @return the new Array
         */
        static Array* create(DataType type, vector<int64_t> shape);

        /**
         * Factory Array creation deriving shape from dims.
         *
         * @param[in] type of Array to create
         * @param[in] dims the shape is taken from the given Dimensions
         * @return the new Array
         */
        static Array* create(DataType type, vector<Dimension*> dims);

        /**
         * Destroys the Array.
         */
        virtual ~Array();

        /**
         * Returns the DataType of the Array.
         *
         * @return the DataType
         */
        virtual DataType get_type() const = 0;

        /**
         * Gets the DataType to use when writing the data.
         */
        virtual DataType get_write_type() const = 0;

        /**
         * Sets the DataType to use when writing the data.
         */
        virtual void set_write_type(DataType type) = 0;

        /**
         * Gets the DataType to use when writing the data.
         */
        virtual DataType get_read_type() const = 0;

        /**
         * Sets the DataType to use when writing the data.
         */
        virtual void set_read_type(DataType type) = 0;

        /**
         * Returns the shape of the Array.
         *
         * @return the shape
         */
        virtual vector<int64_t> get_shape() const = 0;

        /**
         * Returns the size of the Array.
         *
         * @return the global number of elements in the Array
         */
        virtual int64_t get_size() const = 0;

        /**
         * Returns the local shape of the Array portion owned by this process.
         *
         * @return the local shape
         */
        virtual vector<int64_t> get_local_shape() const = 0;

        /**
         * Returns the local size of the Array portion owned by this process.
         *
         * @return the local number of elements in the Array
         */
        virtual int64_t get_local_size() const = 0;

        /**
         * Returns the number of dimensions in this Array.
         *
         * This is also known as the rank of the Array.
         *
         * @return the number of dimensions in this Array
         */
        virtual int64_t get_ndim() const = 0;

        /**
         * Returns true if the number of dimensions is zero.
         *
         * @return true if the number of dimensions is zero.
         */
        virtual bool is_scalar() const = 0;

        /**
         * Fills the array with the given value.
         *
         * @param[in] value to fill with
         */
        virtual void fill(void *value) = 0;

        /**
         * Fills the array with the given value.
         *
         * @param[in] value to fill with
         */
        template <class CT> void fill_value(CT value) {
            DataType type = get_type();
#define DATATYPE_EXPAND(DT,T) \
            if (type == DT) { \
                T the_value = static_cast<T>(value); \
                fill(&the_value); \
            } else
#include "DataType.def"
        }

        /**
         * Copies all elements from the given Array into this Array.
         *
         * The arrays must be the same type and shape.
         *
         * @param[in] src the Array to copy
         */
        virtual void copy(const Array *src) = 0;

        /**
         * Copies elements in a patch of one Array into another one.
         *
         * The patches of Arrays may be of different shapes but must have the
         * same number of elements.  Patches must be nonoverlapping
         * (if src == dst).
         *
         * @param[in] src the Array to copy
         * @param[in] src_lo lower bounds of src Array
         * @param[in] src_hi upper bounds of src Array
         * @param[in] dst_lo lower bounds of dst Array
         * @param[in] dst_hi upper bounds of dst Array
         */
        virtual void copy(const Array *src,
                          const vector<int64_t> &src_lo,
                          const vector<int64_t> &src_hi,
                          const vector<int64_t> &dst_lo,
                          const vector<int64_t> &dst_hi) = 0;

        /**
         * Returns true if the distribution of this Array matches the other.
         *
         * @param[in] other the other Array
         * @return true if the distribution of this Array matches the other.
         */
        virtual bool same_distribution(const Array *other) const = 0;

        /**
         * Returns true if this process owns a local portion of the Array.
         *
         * @return true if this process owns a local portion of the Array.
         */
        virtual bool owns_data() const = 0;

        /**
         * Gets the local distribution of the Array.
         *
         * lo and hi will be empty if this process does not own any data.
         *
         * @param[in,out] lo starting indices for Array section
         * @param[in,out] hi ending indices for Array section
         */
        virtual void get_distribution(vector<int64_t> &lo,
                                      vector<int64_t> &hi) const = 0;

        /**
         * Returns the starting address of the local data
         *
         * This will return NULL if this process doesn't have a local portion
         * Because this method is not const, you MUST call release_update() to
         * indicate the memory is no longer in use (do NOT delete the
         * returned pointer, simply call release_update() when finished).
         *
         * @return the starting address of the local data
         */
        virtual void* access() = 0;

        /**
         * Returns the starting address of the local data
         *
         * This will return NULL if this process doesn't have a local portion
         * Because this method is const, you MUST call release() to
         * indicate the memory is no longer in use (do NOT delete the
         * returned pointer, simply call release() when finished).
         *
         * @return the starting address of the local data
         */
        virtual void* access() const = 0;

        /**
         * Indicate the pointer returned by access() const is no longer in
         * use.
         */
        virtual void release() const = 0;

        /**
         * Indicate the pointer returned by access() is no longer in use.
         * This implies the data was modified (or potentially modified.)
         */
        virtual void release_update() = 0;

        /**
         * Copies all data from Array into local buffer and returns the buffer.
         *
         * Caller must cast the returned buffer to the appropriate type and
         * must also free the returned buffer when finished.
         *
         * The buffer argument is optional.  If not NULL, the buffer should
         * point to an appropriately sized and typed contiguous block of
         * memory.  If NULL, an appropriately sized and typed buffer will be
         * allocated via new.
         *
         * @param[in] buffer to copy into
         * @return the buffer; new if buffer was NULL
         */
        virtual void* get(void *buffer=NULL) const = 0;

        /**
         * Copies data from Array into local buffer and returns the buffer.
         *
         * Caller must cast the returned buffer to the appropriate type and
         * must also free the returned buffer when finished.  Shortcut for a
         * 1-Dimensional get.  It is an error to call this if the rank of the
         * Array is not 1.
         *
         * The buffer argument is optional.  If not NULL, the buffer should
         * point to an appropriately sized and typed contiguous block of
         * memory.  If NULL, an appropriately sized and typed buffer will be
         * allocated via new.
         *
         * @param[in] lo the lowest index in the range to retrieve
         * @param[in] hi the highest index in the range to retrieve
         * @param[in] buffer to copy into
         * @return the buffer; new if buffer was NULL
         */
        virtual void* get(int64_t lo, int64_t hi, void *buffer=NULL) const = 0;

        /**
         * Copies data from Array into local buffer and returns the buffer.
         *
         * Caller must cast the returned buffer to the appropriate type and
         * must also free the returned buffer when finished.
         *
         * The buffer argument is optional.  If not NULL, the buffer should
         * point to an appropriately sized and typed contiguous block of
         * memory.  If NULL, an appropriately sized and typed buffer will be
         * allocated via new.
         *
         * @param[in] lo the lowest indices to retrieve
         * @param[in] hi the highest indices to retrieve
         * @param[in] buffer to copy into
         * @return the buffer; new if buffer was NULL
         */
        virtual void* get(const vector<int64_t> &lo,
                          const vector<int64_t> &hi,
                          void *buffer=NULL) const = 0;

        /**
         * Copies data from the source buffer into into the Array.
         *
         * The buffer is assumed to both have the same number of elements as
         * the entire Array and also to be contiguous.
         *
         * @param[in] buffer the contiguous buffer
         */
        virtual void put(void *buffer) = 0;

        /**
         * Copies data from the source buffer into into the 1-D Array.
         *
         * The buffer is assumed both to have the same number of elements as
         * the indicated patch and also to be contiguous.
         *
         * It is an error to call this if the rank of the Array is not 1.
         *
         * @param[in] buffer the contiguous buffer
         * @param[in] lo
         * @param[in] hi
         */
        virtual void put(void *buffer, int64_t lo, int64_t hi) = 0;

        /**
         * Copies data from the source buffer into into the Array patch.
         *
         * The buffer is assumed to have the same number of elements as the
         * indicated patch.
         *
         * @param[in] buffer the contiguous buffer
         * @param[in] lo
         * @param[in] hi
         */
        virtual void put(void *buffer,
                         const vector<int64_t> &lo,
                         const vector<int64_t> &hi) = 0;

        /**
         * Scatters values into this Array.
         *
         * The order of the values in the provided buffer should match the
         * order of the specified subscripts.
         *
         * @param[in] buffer the values to scatter
         * @param[in] subscripts the indices of where to scatter
         */
        virtual void scatter(void *buffer,
                             vector<int64_t> &subscripts) = 0;

        /**
         * Gathers values from this Array into a local buffer.
         *
         * The order of the values in the returned buffer match the order of
         * the specified subscripts.
         *
         * @param[in] subscripts the indices from where to gather
         * @param[in] buffer contiguous local array to copy values into
         * @return the gathered values
         */
        virtual void* gather(vector<int64_t> &subscripts,
                             void *buffer=NULL) const = 0;

        /**
         * Adds two Arrays.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to add
         * @return the result, as a new Array
         */
        virtual Array* add(const Array *rhs) const = 0;

        /**
         * Adds two Arrays, in-place.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to add
         * @return the result, as this Array
         */
        virtual Array* iadd(const Array *rhs) = 0;

        /**
         * Subtracts two Arrays.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to subtract
         * @return the result, as a new Array
         */
        virtual Array* sub(const Array *rhs) const = 0;

        /**
         * Subtracts two Arrays, in-place.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to subtract
         * @return the result, as this Array
         */
        virtual Array* isub(const Array *rhs) = 0;

        /**
         * Multiplies two Arrays.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to multiply
         * @return the result, as a new Array
         */
        virtual Array* mul(const Array *rhs) const = 0;

        /**
         * Multiplies two Arrays, in-place.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to multiply
         * @return the result, as this Array
         */
        virtual Array* imul(const Array *rhs) = 0;

        /**
         * Divides two Arrays.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to divide
         * @return the result, as a new Array
         */
        virtual Array* div(const Array *rhs) const = 0;

        /**
         * Divides two Arrays, in-place.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to divide
         * @return the result, as this Array
         */
        virtual Array* idiv(const Array *rhs) = 0;

        /**
         * Takes the element-wise maximum of two Arrays.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to consider
         * @return the result, as a new Array
         */
        virtual Array* max(const Array *rhs) const = 0;

        /**
         * Takes the element-wise maximum of two Arrays, in-place.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to consider
         * @return the result, as this Array
         */
        virtual Array* imax(const Array *rhs) = 0;

        /**
         * Takes the element-wise minimum of two Arrays.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to consider
         * @return the result, as a new Array
         */
        virtual Array* min(const Array *rhs) const = 0;

        /**
         * Takes the element-wise minimum of two Arrays, in-place.
         *
         * If the given Array has fewer Dimensions, but the shape is otherwise
         * compatible, the given Array is "broadcast" to fit.
         *
         * @param[in] rhs the Array to consider
         * @return the result, as this Array
         */
        virtual Array* imin(const Array *rhs) = 0;

        /**
         * Computes the element-wise power of the Array.
         *
         * @param[in] exponent to raise each value to
         * @return the result, as a new Array
         */
        virtual Array* pow(double exponent) const = 0;

        /**
         * Computes the element-wise power of the Array, in-place.
         *
         * @param[in] exponent to raise each value to
         * @return the result, as this Array
         */
        virtual Array* ipow(double exponent) = 0;

        /**
         * Sets the fill value for this Array.
         *
         * All operations adhere to fill value.
         *
         * @param[in] value the fill value
         */
        virtual void set_fill_value(double value) = 0;

        /**
         * Returns true if a fill value is set for this Array.
         */
        virtual bool has_fill_value() const = 0;

        /**
         * Returns the fill value.
         *
         * It is an error to call this function without first checking
         * has_fill_value().
         */
        virtual double get_fill_value() const = 0;

        /**
         * Unsets the fill value.
         */
        virtual void clear_fill_value() = 0;

        /**
         * Sets a counter for arithmetic operations (used with fill values).
         *
         * @param[in] counter
         */
        virtual void set_counter(Array *counter) = 0;

        /**
         * Virtual friend implementation of stream insertion operator.
         *
         * @param[in] os the stream
         */
        virtual ostream& print(ostream &os) const = 0;

        friend ostream& operator << (ostream &os, const Array *array);

        /**
         * Dump the contents of this Array to the screen.
         */
        virtual void dump() const = 0;

    protected:
        /**
         * Constructs the Array interface.
         */
        Array();
};

#endif /* ARRAY_H_ */
