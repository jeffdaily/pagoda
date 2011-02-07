#ifndef VALIDATOR_H_
#define VALIDATOR_H_

class Validator
{
    public:
        Validator();
        Validator(const Validator &that);
        virtual ~Validator();

        virtual Validator* clone() const = 0;
        virtual bool is_valid(const void *value) const = 0;
        virtual const void* get_fill_value() const = 0;
};

#endif /* VALIDATOR_H_ */
