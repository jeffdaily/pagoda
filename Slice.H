#ifndef SLICE_H_
#define SLICE_H_

#include <iostream>
    using std::ostream;
#include <sstream>
    using std::istringstream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "RangeException.H"



template <class T>
class Slice;

template <class T>
ostream& operator << (ostream &os, const Slice<T> &other);

/**
 * Eerily similar to Python's slice class.
 *
 * A slice is indicated by a start, stop, and step value. Any or all of those
 * values are allowed to be negative. The indices function is useful for
 * calculating the real index values for each of start, stop, and step given
 * the size of the dimension being sliced.
 */
template <class T>
class Slice
{
    public:
        Slice();
        Slice(T start, T stop, T step);
        Slice(string arg);
        Slice(const Slice<T> &slice);
        virtual ~Slice();

        Slice<T>& operator = (const Slice<T> &other);
        bool operator == (const Slice<T> &other) const;

        void indices(T size, T &start, T &stop, T &step);

        friend ostream& operator << <>(ostream &os, const Slice<T> &other);

    private:
        T start;
        T *stop;
        T *step;
};


template <class T>
Slice<T>::Slice()
    :   start(0)
    ,   stop(NULL)
    ,   step(NULL)
{
}


template <class T>
Slice<T>::Slice(T start, T stop, T step)
    :   start(start)
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
    :   start(0)
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

    if (parts.size() < 1 or parts.size() > 3) {
        throw RangeException("invalid dimension string");
    }

    if (parts.size() > 2) step = new T(strtol(parts[2].c_str(), NULL, 10));
    if (parts.size() > 1) stop = new T(strtol(parts[1].c_str(), NULL, 10));
    start = strtol(parts[0].c_str(), NULL, 10);
}


template <class T>
Slice<T>::Slice(const Slice& slice)
    :   start(slice.start)
    ,   stop(slice.stop == NULL ? NULL : new T(*slice.stop))
    ,   step(slice.step == NULL ? NULL : new T(*slice.step))
{
}


template <class T>
Slice<T>::~Slice()
{
    if (stop) delete stop;
    if (step) delete step;
}


template <class T>
Slice<T>& Slice<T>::operator = (const Slice<T> &other)
{
    if (stop) delete stop;
    if (step) delete step;
    start = other.start;
    stop = other.stop ? new T(*other.stop) : NULL;
    step = other.step ? new T(*other.step) : NULL;
    return *this;
}


template <class T>
bool Slice<T>::operator == (const Slice<T> &other) const
{
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

    return false;
}


template <class T>
ostream& operator << (ostream &os, const Slice<T> &other)
{
    os << "Slice(" << other.start << ",";
    os << (other.stop ? *other.stop : "NULL") << ",";
    os << (other.step ? *other.step : "NULL") << ")";
    return os;
}


/**
 * Calculate and return the real values for start, stop, and step given size.
 *
 * This will convert any missing stop or step value to an appropriate T
 * and convert negative values into positive values although "step" should be
 * unchanged.
 *
 * Examples:
 * "dim,1,5,2", size 20 --> 1,5,2
 * "dim,1,2", size 20 --> 1,2,1
 * "dim,7", size 20 --> 7,8,1
 * "dim,-10,-20,-1", size 20 --> 10,0,-1
 * "dim,-1", size 20 --> 19,20,1
 */
template <class T>
void Slice<T>::indices(T size, T &start, T &stop, T &step)
{
    if (size < 0) throw RangeException("size must be positive");

    start = this->start;
    if (start < 0) start += size;
    if (start < 0 || start > size)
        throw RangeException("start < 0 || start > size");

    if (this->stop) {
        stop = *(this->stop);
        if (stop < 0) stop += size;
        if (stop < 0 || stop > size)
            throw RangeException("stop < 0 || stop > size");
    } else {
        stop = start + 1;
    }

    if (this->step) {
        step = *(this->step);
    } else {
        step = 1;
    }

    if (start > stop && step > 0)
        throw RangeException("start > stop && step > 0 (causes no-op loop)");
    if (start < stop && step < 0)
        throw RangeException("start < stop && step < 0 (causes infinite loop)");
}

#endif // SLICE_H_
