#ifndef DIMENSION_H_
#define DIMENSION_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;


class Dimension
{
    public:
        Dimension();
        Dimension(int ncid, int dimid);
        Dimension(string name, int64_t length, bool unlimited);
        Dimension(const Dimension &copy);
        virtual ~Dimension();

        Dimension& operator = (const Dimension &other);
        bool operator == (const Dimension &other) const;

        string get_name() const;
        int64_t get_size() const;
        bool is_unlimited() const;

        friend ostream& operator << (ostream &os, const Dimension &other);

    private:
        string name;
        int64_t size;
        bool unlimited;
};

#endif // DIMENSION_H_
