#ifndef VALIDMASKCONDITION_H_
#define VALIDMASKCONDITION_H_

#include <sstream>

#include "Validator.H"

using std::istringstream;


template <class T>
class ValidMaskCondition : public Validator
{
    public:
        ValidMaskCondition(const string &op, const string &value);
        ValidMaskCondition(const ValidMaskCondition<T> &that);
        virtual ~ValidMaskCondition();

        virtual ValidMaskCondition<T>* clone() const;
        virtual bool is_valid(const void *value) const;
        virtual const void* get_fill_value() const;

    protected:
        string op;
        T value;
};


template <class T>
ValidMaskCondition<T>::ValidMaskCondition(const string &op, const string &value)
    :   Validator()
    ,   op(op)
    ,   value()
{
    istringstream value_string;
    value_string.str(value);
    value_string >> this->value;
    ASSERT(value_string);
}


template <class T>
ValidMaskCondition<T>::ValidMaskCondition(const ValidMaskCondition<T> &that)
    :   Validator(that)
    ,   op(that.op)
    ,   value(that.value)
{
}


template <class T>
ValidMaskCondition<T>::~ValidMaskCondition()
{
}


template <class T>
ValidMaskCondition<T>* ValidMaskCondition<T>::clone() const
{
    return new ValidMaskCondition<T>(*this);
}


template <class T>
bool ValidMaskCondition<T>::is_valid(const void *value) const
{
    const T tval = *static_cast<const T*>(value);

    if ("<" == op || "lt" == op) {
        return tval < this->value;
    }
    else if ("<=" == op || "le" == op) {
        return tval <= this->value;
    }
    else if (">" == op || "gt" == op) {
        return tval > this->value;
    }
    else if (">=" == op || "ge" == op) {
        return tval >= this->value;
    }
    else if ("=" == op || "eq" == op) {
        return tval == this->value;
    }
    else if ("!=" == op || "ne" == op) {
        return tval != this->value;
    }
    
    ERR("bad mask comparator");
}


template <class T>
const void* ValidMaskCondition<T>::get_fill_value() const
{
    /* TODO: this might not be the right thing to do? */
    return &value;
}

#endif /* VALIDMASKCONDITION_H_ */
