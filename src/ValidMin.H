#ifndef VALIDMIN_H_
#define VALIDMIN_H_

#include "Validator.H"

template <class T>
class ValidMin : public Validator
{
    public:
        ValidMin(const T &value, bool is_fill=false);
        ValidMin(const T &range_value, const T &fill_value);
        ValidMin(const ValidMin<T> &that);
        virtual ~ValidMin();

        virtual bool is_valid(const void *value) const;
        virtual ValidMin<T>* clone() const;
        virtual const void* get_fill_value() const;

    protected:
        T range_value;
        T fill_value;
};


template <class T>
static T get_min_range_from_fill(const T &fill)
{
    return fill;
}


template <class T>
static T get_fill_from_min_range(const T &range)
{
    return range;
}


template <class T>
ValidMin<T>::ValidMin(const T &value, bool is_fill)
    :   Validator()
    ,   range_value(is_fill? get_min_range_from_fill(value) : value)
    ,   fill_value( is_fill? value : get_fill_from_min_range(value))
{
}


template <class T>
ValidMin<T>::ValidMin(const T &range, const T &fill)
    :   Validator()
    ,   range_value(range)
    ,   fill_value(fill)
{
}


template <class T>
ValidMin<T>::ValidMin(const ValidMin<T> &that)
    :   Validator(that)
    ,   range_value(that.range_value)
    ,   fill_value(that.fill_value)
{
}


template <class T>
ValidMin<T>::~ValidMin()
{
}


template <class T>
ValidMin<T>* ValidMin<T>::clone() const
{
    return new ValidMin<T>(*this);
}


template <class T>
bool ValidMin<T>::is_valid(const void *value) const
{
    return range_value <= *static_cast<const T*>(value);
}


template <class T>
const void* ValidMin<T>::get_fill_value() const
{
    return &fill_value;
}

#endif /* VALIDMIN_H_ */
