#ifndef TYPEDVALUES_H_
#define TYPEDVALUES_H_

#include <iostream>
    using std::ostream;
#include <sstream>
    using std::ostringstream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;


template <class T>
class TypedValues
    :   public vector<T>
    ,   public Values
{
    public:
        TypedValues();
        TypedValues(const T &value, const size_t &num=1);
        TypedValues(T *values, const size_t &num);
        TypedValues(const vector<T> &values);
        virtual ~TypedValues();

        virtual TypedValues<T>* clone() const;

        virtual size_t size() const;
        virtual ostream& print(ostream &os) const;

        virtual void as(size_t n, char &val) const;
        virtual void as(size_t n, signed char &val) const;
        virtual void as(size_t n, unsigned char &val) const;
        virtual void as(size_t n, short &val) const;
        virtual void as(size_t n, int &val) const;
        virtual void as(size_t n, long &val) const;
        virtual void as(size_t n, float &val) const;
        virtual void as(size_t n, double &val) const;
        virtual void as(size_t n, string &val) const;
};


template <class T>
static ostream& __print(ostream &os, const TypedValues<T> &obj)
{
    size_t i,limit;
    for (i=0,limit=obj.size(); i<limit-1; ++i) {
        os << obj.at(i) << ", ";
    }
    if (limit > 0) {
        os << obj.at(limit-1);
    }
    return os;
}


template <>
static ostream& __print(ostream &os, const TypedValues<char> &obj)
{
    //os << '"';
    size_t i,limit;
    for (i=0,limit=obj.size(); i<limit; ++i) {
        char val = obj.at(i);
        if (val == '\0') break;
        os << val;
    }
    //os << '"';
    return os;
}


template <class T>
TypedValues<T>::TypedValues()
    :   vector<T>()
{
}


template <class T>
TypedValues<T>::TypedValues(const T &value, const size_t &num)
    :   vector<T>(num, value)
{
}


template <class T>
TypedValues<T>::TypedValues(T *values, const size_t &num)
    :   vector<T>(values, values+num)
{
}


template <class T>
TypedValues<T>::TypedValues(const vector<T> &values)
    :   vector<T>(values)
{
}


template <class T>
TypedValues<T>::~TypedValues()
{
}


template <class T>
TypedValues<T>* TypedValues<T>::clone() const
{
    return new TypedValues(*this);
}


template <class T>
size_t TypedValues<T>::size() const
{
    return vector<T>::size();
}


template <class T>
ostream& TypedValues<T>::print(ostream &os) const
{
    return __print(os, *this);
}


#define implement_as(TYPE) \
template <class T> \
void TypedValues<T>::as(size_t n, TYPE &val) const \
{ \
    val = (TYPE)this->at(n); \
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
    os << this->at(n);
    val = os.str();
}

#endif // TYPEDVALUES_H_
