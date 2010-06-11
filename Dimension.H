#ifndef DIMENSION_H_
#define DIMENSION_H_

#include <iostream>
#include <string>

using std::ostream;
using std::string;


class Dimension
{
    public:
        Dimension();
        virtual ~Dimension();

        virtual string get_name() const = 0;
        virtual int64_t get_size() const = 0;
        virtual bool is_unlimited() const = 0;

        virtual ostream& print(ostream &os) const = 0;

        friend ostream& operator << (ostream &os, const Dimension *other);

    protected:
        Dimension(const Dimension &copy);
        Dimension& operator = (const Dimension &copy);
};

#endif // DIMENSION_H_
