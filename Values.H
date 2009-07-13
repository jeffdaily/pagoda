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

        virtual void as(size_t n, char &val) const = 0;
        virtual void as(size_t n, signed char &val) const = 0;
        virtual void as(size_t n, unsigned char &val) const = 0;
        virtual void as(size_t n, short &val) const = 0;
        virtual void as(size_t n, int &val) const = 0;
        virtual void as(size_t n, long &val) const = 0;
        virtual void as(size_t n, float &val) const = 0;
        virtual void as(size_t n, double &val) const = 0;
        virtual void as(size_t n, string &val) const = 0;

        virtual void as(char* &values) const;
        virtual void as(signed char* &values) const;
        virtual void as(unsigned char* &values) const;
        virtual void as(short* &values) const;
        virtual void as(int* &values) const;
        virtual void as(long* &values) const;
        virtual void as(float* &values) const;
        virtual void as(double* &values) const;
        virtual void as(string &values) const;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Values *values);
};

#endif // VALUES_H_
