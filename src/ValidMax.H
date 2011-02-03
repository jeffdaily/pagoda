#ifndef VALIDMAX_H_
#define VALIDMAX_H_

#include "Validator.H"

template <class T>
class ValidMax : public Validator
{
    public:
        ValidMax(const T &value, bool is_fill=false);
        ValidMax(const T &range_value, const T &fill_value);
        ValidMax(const ValidMax<T> &that);
        virtual ~ValidMax();

        virtual bool is_valid(void *value) const;
        virtual ValidMax<T>* clone() const;
        virtual const void* get_fill_value() const;

    protected:
        T range_value;
        T fill_value;
};


template <class T>
static T get_max_range_from_fill(const T &fill)
{
    return fill;
}


template <class T>
static T get_fill_from_max_range(const T &range)
{
    return range;
}


template <class T>
ValidMax<T>::ValidMax(const T &value, bool is_fill)
    :   Validator()
    ,   range_value(is_fill? get_max_range_from_fill(value) : value)
    ,   fill_value( is_fill? value : get_fill_from_max_range(value))
{
}


template <class T>
ValidMax<T>::ValidMax(const T &range, const T &fill)
    :   Validator()
    ,   range_value(range)
    ,   fill_value(fill)
{
}


template <class T>
ValidMax<T>::ValidMax(const ValidMax<T> &that)
    :   Validator(that)
    ,   range_value(that.range_value)
    ,   fill_value(that.fill_value)
{
}


template <class T>
ValidMax<T>::~ValidMax()
{
}


template <class T>
ValidMax<T>* ValidMax<T>::clone() const
{
    return new ValidMax<T>(*this);
}


template <class T>
bool ValidMax<T>::is_valid(void *value) const
{
    return range_value >= *static_cast<T*>(value);
}


template <class T>
const void* ValidMax<T>::get_fill_value() const
{
    return &fill_value;
}

#endif /* VALIDMAX_H_ */
