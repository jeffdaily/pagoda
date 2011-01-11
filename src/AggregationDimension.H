#ifndef AGGREGATIONDIMENSION_H_
#define AGGREGATIONDIMENSION_H_

#include <stdint.h>

#include <iostream>
#include <string>
#include <vector>

using std::ostream;
using std::string;
using std::vector;

#include "Dimension.H"

class Aggregation;
class Dataset;


/**
 * A Dimension which concatenates multiple Dimensions together.
 */
class AggregationDimension : public Dimension
{
    public:
        AggregationDimension(Aggregation *agg, Dimension *dim);
        virtual ~AggregationDimension();

        void add(Dimension *dim);

        virtual string get_name() const;
        virtual int64_t get_size() const;
        virtual bool is_unlimited() const;
        virtual Dataset* get_dataset() const;

        virtual ostream& print(ostream &os) const;

    protected:
        string name;
        int64_t size;
        bool _is_unlimited;
        Aggregation *agg;
};

#endif // AGGREGATIONDIMENSION_H_
