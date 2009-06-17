#ifndef DIMENSIONSLICE_H_
#define DIMENSIONSLICE_H_

#include "Slice.H"


template <class T>
class DimensionSlice;

template <class T>
ostream& operator << (ostream &os, const DimensionSlice<T> &other);

/**
 * A named Slice<T>.
 */
template <class T>
class DimensionSlice : public Slice<T>
{
    public:
        DimensionSlice();
        DimensionSlice(string name, T start, T stop, T step);
        DimensionSlice(string arg);
        DimensionSlice(const DimensionSlice<T> &copy);
        virtual ~DimensionSlice();

        DimensionSlice<T>& operator = (const DimensionSlice<T> &other);
        bool operator == (const DimensionSlice<T> &other) const;

        friend ostream& operator << <>(ostream &os, const DimensionSlice<T> &other);

        string get_name() const;

    private:
        string name;
};


template <class T>
DimensionSlice<T>::DimensionSlice()
    :   Slice<T>()
    ,   name("")
{
}


template <class T>
DimensionSlice<T>::DimensionSlice(string name, T start, T stop, T step)
    :   Slice<T>(start, stop, step)
    ,   name(name)
{
}


/**
 * Constructor that parses the string argument.
 *
 * Examples:
 * "dim,1,5,2"
 * "dim,1,2"
 * "dim,7"
 * "dim,-10,-20,-1"
 * "dim,-1"
 */
template <class T>
DimensionSlice<T>::DimensionSlice(string arg)
    :   Slice<T>(arg.substr(arg.find(",")+1))
    ,   name(arg.substr(0,arg.find(",")))
{
}


template <class T>
DimensionSlice<T>::DimensionSlice(const DimensionSlice& copy)
    :   Slice<T>(copy)
    ,   name(copy.name)
{
}


template <class T>
DimensionSlice<T>::~DimensionSlice()
{
}


template <class T>
DimensionSlice<T>& DimensionSlice<T>::operator = (const DimensionSlice<T> &other)
{
    (void) Slice<T>::operator = (other);
    name = other.name;
    return *this;
}


template <class T>
bool DimensionSlice<T>::operator == (const DimensionSlice<T> &other) const
{
    return name == other.name && Slice<T>::operator == (other);
}


template <class T>
ostream& operator << (ostream &os, const DimensionSlice<T> &other)
{
    os << "DimensionSlice(";
    os << other.name << ",";
    os << other.start << ",";
    os << (other.stop ? *other.stop : "NULL") << ",";
    os << (other.step ? *other.step : "NULL") << ")";
    return os;
}


template <class T>
string DimensionSlice<T>::get_name() const
{
    return name;
}

#endif // DIMENSIONSLICE_H_
