#ifndef DIMENSION_H_
#define DIMENSION_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;


class Mask;


class Dimension
{
    public:
        Dimension();
        virtual ~Dimension();

        virtual void set_mask(Mask *mask);
        virtual Mask* get_mask() const;

        virtual string get_name() const = 0;
        virtual int64_t get_size() const = 0;
        virtual bool is_unlimited() const = 0;

        virtual ostream& print(ostream &os) const = 0;

        friend ostream& operator << (ostream &os, const Dimension *other);

    protected:
        Dimension(const Dimension &copy);
        Dimension& operator = (const Dimension &copy);

        Mask *mask;
};

#endif // DIMENSION_H_
