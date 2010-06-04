#ifndef ARRAY_H_
#define ARRAY_H_

#include <vector>

using std::vector;

#include "DataType.H"
#include "Dimension.H"

class Array
{
    public:
        Array(DataType type, vector<int64_t> shape);
        Array(DataType type, vector<Dimension*> dims);
        Array(const Array &that);

        virtual ~Array();

        DataType get_type() const;
        vector<int64_t> get_shape() const;
        size_t get_ndim() const;

        Array& operator=(const Array &that);
        Array& operator+=(const Array &that);
        Array& operator-=(const Array &that);
        Array& operator*=(const Array &that);
        Array& operator/=(const Array &that);

    protected:
        void get_distribution();
        bool has_data() const;
        int64_t get_count() const;

        int handle;
        DataType type;
        vector<int64_t> shape;
        vector<int64_t> lo;
        vector<int64_t> hi;
};

#endif /* ARRAY_H_ */
