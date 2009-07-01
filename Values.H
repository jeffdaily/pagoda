#ifndef VALUES_H_
#define VALUES_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;


class Values
{
    public:
        Values();
        virtual ~Values();

        virtual Values* clone() const = 0;

        virtual size_t size() const = 0;

        virtual void as(size_t n, char &val) const = 0;   // nth value as char
        virtual void as(size_t n, short &val) const = 0;  // nth value as short
        virtual void as(size_t n, int &val) const = 0;    // nth value as int
        virtual void as(size_t n, long &val) const = 0;   // nth value as long
        virtual void as(size_t n, float &val) const = 0;  // nth value as float
        virtual void as(size_t n, double &val) const = 0; // nth value as double
        virtual void as(size_t n, string &val) const = 0; // value as string

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Values *values);
};

#endif // VALUES_H_
