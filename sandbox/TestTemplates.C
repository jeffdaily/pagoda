#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <sstream>
#include <vector>

using namespace std;

template <class Collection>
string vec_to_string(
        const Collection &collection,
        char const * delimiter=",",
        const string &name="")
{
    typedef typename Collection::const_iterator iter;
    std::ostringstream os;
    iter beg = collection.begin();
    iter end = collection.end();

    if (!name.empty()) {
        os << name << "=";
    }

    if (beg == end) {
        os << "{}";
    } else {
        os << "{" << *(beg++);
        for (; beg != end; ++beg) {
            os << delimiter << *beg;
        }
        os << "}";
    }

    return os.str();
}
#define STR_VEC(vec) vec_to_string(vec,",",#vec)


int64_t shape_to_size(const vector<int64_t> &shape)
{
    return accumulate(shape.begin(),shape.end(),1,multiplies<int64_t>());
}   


template <class T>
class Iterator {
    public:

        typedef void(*forward_callback)(T*);
        typedef void(*backward_callback)(int64_t);

        Iterator(T *buf_, const vector<int64_t> &shape_)
            :   buf(buf_)
            ,   shape(shape_)
            ,   size(accumulate(
                        shape.begin(), shape.end(), 1, multiplies<int64_t>()))
            ,   ndim(shape.size())
            ,   nd_m1(ndim-1)
            ,   coords(shape.size())
            ,   dims_m1(shape.size())
            ,   strides(shape.size())
            ,   backstrides(shape.size())
            ,   position(0)
            ,   fwd(NULL)
            ,   bck(NULL)
        {
            for (int64_t i=nd_m1; i>=0; --i) {
                coords[i] = 0;
                dims_m1[i] = shape[i]-1;
                strides[i] = (i == nd_m1) ? 1 : strides[i+1]*shape[i+1];
                backstrides[i] = dims_m1[i]*strides[i];
            }
        }

        void reset() {
            position = 0;
        }

        void next() {
            assert(has_next());
            ++position;
            for (int64_t i=nd_m1; i>=0; --i) {
                if (coords[i] < dims_m1[i]) {
                    ++coords[i];
                    buf += strides[i];
                    break;
                }
                else {
                    coords[i] = 0;
                    buf -= backstrides[i];
                    if (bck) {
                        bck(i);
                    }
                }
            }
        }

        bool has_next() {
            return position < size;
        }

        T *buf;
        vector<int64_t> shape;
        int64_t size;
        int64_t ndim;
        int64_t nd_m1;
        vector<int64_t> coords;
        vector<int64_t> dims_m1;
        vector<int64_t> strides;
        vector<int64_t> backstrides;
        int64_t position;
        forward_callback fwd;
        backward_callback bck;
};


template <class T>
class Array {
    protected:
        static void bck(int64_t i) {
            cout << endl;
        }

    public:
        Array(const vector<int64_t> &shape_) {
            shape = shape_;
            size = shape_to_size(shape_);
            data = new T[shape_to_size(shape_)];
        }

        ~Array() {
            delete [] data;
        }

        template <class U>
        Array<T>* add(const Array<U> &rhs) {
            Array<T> *result = new Array<T>(shape);
            const U *rhs_data = rhs.get_data();
            assert(size == rhs.get_size());
            for (size_t i=0; i<size; ++i) {
                result->data[i] = rhs_data[i];
            }
            return result;
        }

        void enumerate(const T &start) {
            for (size_t i=0; i<size; ++i) {
                data[i] = start*(i+1);
            }
        }

        void pretty_print() {
            Iterator<T> iter(data, shape);
            iter.bck = bck;
            while (iter.has_next()) {
                cout << *iter.buf << ", ";
                iter.next();
            }
        }

        void print() {
            Iterator<T> iter(data, shape);
            while (iter.has_next()) {
                cout << STR_VEC(iter.coords) << " " << *iter.buf << endl;
                iter.next();
            }
        }

        const T* get_data() const {
            return data;
        }

        vector<int64_t> get_shape() const {
            return shape;
        }

        int64_t get_size() const {
            return size;
        }

    private:
        T *data;
        vector<int64_t> shape;
        int64_t size;
};


int main(int argc, char **argv)
{
    vector<int64_t> shape1;
    shape1.push_back(3);
    shape1.push_back(4);
    shape1.push_back(5);
    vector<int64_t> shape2;
    shape2.push_back(5);
    shape2.push_back(4);
    shape2.push_back(3);

    Array<double> darray(shape1);
    Array<float> farray(shape2);

    darray.enumerate(1.0);
    farray.enumerate(2.0);

    Array<double> *result = darray.add(farray);

    darray.pretty_print();
    result->pretty_print();

    delete result;

    return EXIT_SUCCESS;
}
