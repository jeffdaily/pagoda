#ifndef VALUES_H_
#define VALUES_H_

#include <iostream>
#include <string>
#include <vector>

using std::ostream;
using std::string;
using std::vector;


class Values
{
    public:
        virtual ~Values();

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

        void as(vector<char> &values) const;
        void as(vector<signed char> &values) const;
        void as(vector<unsigned char> &values) const;
        void as(vector<short> &values) const;
        void as(vector<int> &values) const;
        void as(vector<long> &values) const;
        void as(vector<float> &values) const;
        void as(vector<double> &values) const;
        void as(string &values) const;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Values *values);

    protected:
        Values();
};

#endif // VALUES_H_
