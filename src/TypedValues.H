#ifndef TYPEDVALUES_H_
#define TYPEDVALUES_H_

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "Values.H"

using std::ostream;
using std::ostringstream;
using std::string;
using std::vector;


/**
 * A typed, local array.
 *
 * Often used to hold Attribute values.
 */
template <class T>
class TypedValues : public Values
{
    public:
        TypedValues(T *values, const size_t &size);
        TypedValues(const vector<T> &values);
        virtual ~TypedValues();

        virtual size_t size() const;

        virtual void as(size_t n, char &val) const;
        virtual void as(size_t n, signed char &val) const;
        virtual void as(size_t n, unsigned char &val) const;
        virtual void as(size_t n, short &val) const;
        virtual void as(size_t n, int &val) const;
        virtual void as(size_t n, long &val) const;
        virtual void as(size_t n, float &val) const;
        virtual void as(size_t n, double &val) const;
        virtual void as(size_t n, string &val) const;

        virtual TypedValues<T>& operator += (const Values *values);

        virtual ostream& print(ostream &os) const;

    protected:
        vector<T> values;
};


template <class T>
TypedValues<T>::TypedValues(T *values, const size_t &size)
    :   Values()
    ,   values(values, values+size)
{
}


template <class T>
TypedValues<T>::TypedValues(const vector<T> &values)
    :   Values()
    ,   values(values)
{
}


template <class T>
TypedValues<T>::~TypedValues()
{
}


template <class T>
size_t TypedValues<T>::size() const
{
    return values.size();
}


#define implement_as(TYPE) \
template <class T> \
void TypedValues<T>::as(size_t n, TYPE &val) const \
{ \
    val = (TYPE)values.at(n); \
}
implement_as(char)
implement_as(signed char)
implement_as(unsigned char)
implement_as(short)
implement_as(int)
implement_as(long)
implement_as(float)
implement_as(double)
#undef implement_as


template <class T>
void TypedValues<T>::as(size_t n, string &val) const
{
    ostringstream os;
    os << values.at(n);
    val = os.str();
}


template <class T>
TypedValues<T>& TypedValues<T>::operator += (const Values *other)
{
    vector<T> other_values;
    other->as(other_values);
    values.insert(values.end(), other_values.begin(), other_values.end());
    return *this;
}


template <class T>
ostream& TypedValues<T>::print(ostream &os) const
{
    size_t i = 0;
    size_t limit = values.size();
    for (i=0; i<limit-1; ++i) {
        os << values.at(i) << ", ";
    }
    if (limit > 0) {
        os << values.at(limit-1);
    }
    return os;
}

#endif // TYPEDVALUES_H_
