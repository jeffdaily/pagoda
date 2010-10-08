#ifndef SLICE_H_
#define SLICE_H_

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "RangeException.H"

using std::ostream;
using std::istringstream;
using std::string;
using std::strtol;
using std::vector;


// predeclare Slice and friend operator to avoid compiler confusion
template <class T> class Slice;
template <class T> ostream& operator<< (ostream &os, const Slice<T> &other);


/**
 * Eerily similar to Python's slice class, but with a 'name' field.
 *
 * A slice is indicated by a start, stop, and step value. Any or all of those
 * values are allowed to be negative. The indices function is useful for
 * calculating the real index values for each of start, stop, and step given
 * the size of the dimension being sliced.
 *
 * The value for start is inclusive while the value for stop is exclusive.
 */
template <class T>
class Slice
{
    public:
        Slice();
        Slice(const string &name, T start, T stop, T step);
        Slice(string arg);
        Slice(const Slice<T> &slice);
        //Slice<T>& operator= (const Slice<T> &other);
        virtual ~Slice();

        bool operator== (const Slice<T> &other) const;

        string get_name() const;
        void indices(T size, T &start, T &stop, T &step) const;
        void indices_exclusive(T size, T &start, T &stop, T &step) const;

        friend ostream& operator<< <>(ostream &os, const Slice<T> &other);

    protected:
        string name;
        T start;
        T *stop;
        T *step;
};


template <class T>
Slice<T>::Slice()
    :   name("")
    ,   start(0)
    ,   stop(NULL)
    ,   step(NULL)
{
}


template <class T>
Slice<T>::Slice(const string &name, T start, T stop, T step)
    :   name(name)
    ,   start(start)
    ,   stop(new T(stop))
    ,   step(new T(step))
{
}


/**
 * Constructor that parses the string argument.
 *
 * Examples:
 * "1,5,2"
 * "1,2"
 * "7"
 * "-10,-20,-1"
 * "-1"
 */
template <class T>
Slice<T>::Slice(string arg)
    :   name("")
    ,   start(0)
    ,   stop(NULL)
    ,   step(NULL)
{
    vector<string> parts;
    istringstream ss(arg);
    string token;
    while (!ss.eof()) {
        getline(ss, token, ',');
        parts.push_back(token);
    }

    if (parts.size() < 2 or parts.size() > 4) {
        throw RangeException(string("invalid dimension string"));
    }

    name = parts[0];
    if (parts.size() > 3) step = new T(strtol(parts[3].c_str(), NULL, 10));
    if (parts.size() > 2) stop = new T(strtol(parts[2].c_str(), NULL, 10));
    start = strtol(parts[1].c_str(), NULL, 10);
}


template <class T>
Slice<T>::Slice(const Slice& slice)
    :   name(slice.name)
    ,   start(slice.start)
    ,   stop(slice.stop == NULL ? NULL : new T(*slice.stop))
    ,   step(slice.step == NULL ? NULL : new T(*slice.step))
{
}


/*
template <class T>
Slice<T>& Slice<T>::operator= (const Slice<T> &other)
{
    name = other.name;
    if (stop) delete stop;
    if (step) delete step;
    start = other.start;
    stop = other.stop ? new T(*other.stop) : NULL;
    step = other.step ? new T(*other.step) : NULL;
    return *this;
}
*/


template <class T>
Slice<T>::~Slice()
{
    if (stop) delete stop;
    if (step) delete step;
}


template <class T>
bool Slice<T>::operator== (const Slice<T> &other) const
{
    if (name == other.name) {
        if (start == other.start) {
            if (!stop && !step) {
                return true;
            }
            if (!step) {
                return *stop == *other.stop;
            }
            if (!stop) {
                return *step == *other.step;
            }
            return *stop == *other.stop && *step == *other.step;
        }
    }

    return false;
}


template <class T>
ostream& operator<< (ostream &os, const Slice<T> &other)
{
    os << "Slice("
        << other.name << ","
        << other.start << ",";
    if (other.stop) {
        os << *other.stop;
    } else {
        os << "NULL";
    }
    os << ",";
    if (other.step) {
        os << *other.step;
    } else {
        os << "NULL";
    }
    os << ")";
    return os;
}


template <class T>
string Slice<T>::get_name() const
{
    return name;
}


/**
 * Calculate and return the real values for start, stop, and step given size.
 *
 * This will convert any missing stop or step value to an appropriate T
 * and convert negative values into positive values although "step" should be
 * unchanged.
 *
 * The returned value for stop is inclusive.
 *
 * Examples:
 * "dim,1,5,2", size 20 --> 1,5,2
 * "dim,1,2", size 20 --> 1,2,1
 * "dim,7", size 20 --> 7,7,1
 * "dim,-10,-20,-1", size 20 --> 10,0,-1
 * "dim,-1", size 20 --> 19,19,1
 */
template <class T>
void Slice<T>::indices(T size, T &_start, T &_stop, T &_step) const
{
    if (size < 0) throw RangeException(string("size must be positive"));

    _start = this->start;
    if (_start < 0) _start += size;
    if (_start < 0 || _start >= size)
        throw RangeException(string("start < 0 || start >= size"));

    if (this->stop) {
        _stop = *(this->stop);
        if (_stop < 0) _stop += size;
        if (_stop < 0 || _stop > size)
            throw RangeException(string("stop < 0 || stop > size"));
    } else {
        _stop = _start;
    }

    if (this->step) {
        _step = *(this->step);
    } else {
        _step = 1;
    }

    if (_start > _stop && _step > 0)
        throw RangeException(
                string("start > stop && step > 0 (causes no-op loop)"));
    if (_start < _stop && _step < 0)
        throw RangeException(
                string("start < stop && step < 0 (causes infinite loop)"));
}


/**
 * Calculate and return the real values for start, stop, and step given size.
 *
 * This will convert any missing stop or step value to an appropriate T
 * and convert negative values into positive values although "step" should be
 * unchanged.
 *
 * The returned value for stop is exclusive.
 *
 * Examples:
 * "dim,1,5,2", size 20 --> 1,5,2
 * "dim,1,2", size 20 --> 1,2,1
 * "dim,7", size 20 --> 7,8,1
 * "dim,-10,-20,-1", size 20 --> 10,0,-1
 * "dim,-1", size 20 --> 19,20,1
 */
template <class T>
void Slice<T>::indices_exclusive(T size, T &_start, T &_stop, T &_step) const
{
    if (size < 0) throw RangeException(string("size must be positive"));

    _start = this->start;
    if (_start < 0) _start += size;
    if (_start < 0 || _start >= size)
        throw RangeException(string("start < 0 || start >= size"));

    if (this->stop) {
        _stop = *(this->stop);
        if (_stop < 0) _stop += size;
        if (_stop < 0 || _stop > size)
            throw RangeException(string("stop < 0 || stop > size"));
    } else {
        _stop = _start + 1;
    }

    if (this->step) {
        _step = *(this->step);
    } else {
        _step = 1;
    }

    if (_start > _stop && _step > 0)
        throw RangeException(
                string("start > stop && step > 0 (causes no-op loop)"));
    if (_start < _stop && _step < 0)
        throw RangeException(
                string("start < stop && step < 0 (causes infinite loop)"));
}

typedef Slice<int64_t> DimSlice;

#endif // SLICE_H_
