#ifndef ARRAY_H_
#define ARRAY_H_

#include <vector>

using std::vector;

#include "DataType.H"
#include "Dimension.H"


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
         * Factory Array creation.
         *
         * @param[in] type the type of Array to create
         * @param[in] shape the shape of the Array to create
         */
        static Array* create(DataType type, vector<int64_t> shape);

        /**
         * Factory Array creation.
         *
         * @param[in] type the type of Array to create
         * @param[in] dims the shape is take from the given Dimensions
         */
        static Array* create(DataType type, vector<Dimension*> dims);

        /**
         * Array destructor.
         */
        virtual ~Array();

        /**
         * Return the DataType of the Array.
         *
         * @return the DataType
         */
        virtual DataType get_type() const = 0;

        /**
         * Return the shape of the Array.
         *
         * @return the shape
         */
        virtual vector<int64_t> get_shape() const = 0;

        /**
         * Return the size of the Array.
         *
         * @return the global number of elements in the Array
         */
        virtual int64_t get_size() const;

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
        virtual int64_t get_local_size() const;

        /**
         * Returns the number of dimensions in this Array.
         *
         * This is also known as the rank of the Array.
         *
         * @return the number of dimensions in this Array
         */
        virtual int64_t get_ndim() const = 0;

        /**
         * Fills the array with the given value.
         *
         * The value must point to the appropriate type.
         *
         * @param[in] value the value to fill with
         */
        virtual void fill(void *value) = 0;

        /**
         * Copies all elements from the given Array into this Array.
         * 
         * The arrays must be the same type, shape, and identically aligned.
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
        virtual bool same_distribution(const Array *other) const;

        /**
         * Returns true if this process owns a local portion of the Array.
         *
         * @return true if this process owns a local portion of the Array.
         */
        virtual bool owns_data() const = 0;

        /**
         * Get the local distribution of the Array.
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
         * This implies the data was modified (or potentially modified.
         */
        virtual void release_update() = 0;

        /**
         * Copies data from Array into local buffer and returns the buffer.
         * Caller must cast the returned buffer to the appropriate type and
         * must also free the returned buffer when finished.  Shortcut for a
         * 1-Dimensional get.
         * 
         * @param[in] lo
         * @param[in] hi
         * @return the buffer
         */
        virtual void* get(int64_t lo, int64_t hi) const;

        /**
         * Copies data from Array into local buffer and returns the buffer.
         * Caller must cast the returned buffer to the appropriate type and
         * must also free the returned buffer when finished.
         * 
         * @param[in] lo
         * @param[in] hi
         * @return the buffer
         */
        virtual void* get(const vector<int64_t> &lo,
                          const vector<int64_t> &hi) const;

        /**
         * Copies data from Array into given buffer and returns the buffer.
         * Caller must cast the returned buffer to the appropriate type.
         * 
         * @param[in,out] buffer the local array
         * @param[in] lo
         * @param[in] hi
         * @param[in] ld array specifying the leading
         *               dimensions/strides/extents of the buffer array
         * @return the buffer
         */
        virtual void* get(void *buffer,
                          const vector<int64_t> &lo,
                          const vector<int64_t> &hi,
                          const vector<int64_t> &ld) const = 0;

        /**
         * Copies data from the source buffer into into the Array.
         *
         * The buffer is assumed to have the same number of elements as the
         * indicated patch.
         *
         * @param[in] buffer the buffer
         * @param[in] lo
         * @param[in] hi
         */
        virtual void put(void *buffer,
                         const vector<int64_t> &lo,
                         const vector<int64_t> &hi);

        /**
         * Copies data from the source buffer into into the Array.
         *
         * The buffer is assumed to have the same number of elements as the
         * indicated patch.
         *
         * @param[in] buffer the buffer
         * @param[in] lo
         * @param[in] hi
         * @param[in] ld the leading dimensions of the buffer array
         */
        virtual void put(void *buffer,
                         const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         const vector<int64_t> &ld) = 0;

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
                             const vector<int64_t> &subscripts) = 0;

        /**
         * Gathers values from this Array into a local buffer.
         *
         * The order of the values in the returned buffer match the order of
         * the specified subscripts.
         *
         * @param[in] subscripts the indices from where to gather
         * @return the gathered values
         */
        virtual void* gather(const vector<int64_t> &subscripts) const = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Array *array);

        /**
         * Dump the contents of this Array to the screen.
         */
        virtual void dump() const = 0;

    protected:
        Array();
};

#endif /* ARRAY_H_ */
