#ifndef VALIDRANGE_H_
#define VALIDRANGE_H_

#include "Validator.H"

template <class T>
class ValidRange : public Validator
{
    public:
        ValidRange(const T &min, const T &max);
        ValidRange(const T &min, const T &max, const T &fill);
        ValidRange(const ValidRange<T> &that);
        virtual ~ValidRange();

        virtual ValidRange<T>* clone() const;
        virtual bool is_valid(void *value) const;
        virtual const void* get_fill_value() const;

    protected:
        T min;
        T max;
        T fill;
};


template <class T>
static T get_fill_from_range(const T &min, const T &max)
{
    return min-1;
}


template <class T>
ValidRange<T>::ValidRange(const T &min, const T &max)
    :   Validator()
    ,   min(min)
    ,   max(max)
    ,   fill(get_fill_from_range(min,max))
{
}


template <class T>
ValidRange<T>::ValidRange(const T &min, const T &max, const T &fill)
    :   Validator()
    ,   min(min)
    ,   max(max)
    ,   fill(fill)
{
}


template <class T>
ValidRange<T>::ValidRange(const ValidRange<T> &that)
    :   Validator(that)
    ,   min(that.min)
    ,   max(that.max)
    ,   fill(that.fill)
{
}


template <class T>
ValidRange<T>::~ValidRange()
{
}


template <class T>
ValidRange<T>* ValidRange<T>::clone() const
{
    return new ValidRange<T>(*this);
}


template <class T>
bool ValidRange<T>::is_valid(void *value) const
{
    T tval = *static_cast<T*>(value);
    return min <= tval && tval <= max;
}


template <class T>
const void* ValidRange<T>::get_fill_value() const
{
    return &fill;
}

#endif /* VALIDRANGE_H_ */
