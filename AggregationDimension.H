#ifndef AGGREGATIONDIMENSION_H_
#define AGGREGATIONDIMENSION_H_

#include <iostream>
#include <string>
#include <vector>

#include "Dimension.H"

using std::ostream;
using std::string;
using std::vector;


class AggregationDimension : public Dimension
{
    public:
        AggregationDimension(Dimension *dim);
        virtual ~AggregationDimension();

        void add(Dimension *dim);

        virtual string get_name() const;
        virtual int64_t get_size() const;
        virtual bool is_unlimited() const;

        virtual ostream& print(ostream &os) const;

    protected:
        string name;
        int64_t size;
        bool _is_unlimited;
};

#endif // AGGREGATIONDIMENSION_H_
