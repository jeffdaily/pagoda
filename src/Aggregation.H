#ifndef AGGREGATION_H_
#define AGGREGATION_H_

#include <iostream>
#include <vector>

using std::ostream;
using std::vector;

#include "Dataset.H"

class Attribute;
class Dimension;
class Variable;


/**
 * Interface for an aggregated Dataset.
 */
class Aggregation : public Dataset
{
    public:
        virtual ~Aggregation();

        virtual void add(Dataset *dataset) = 0;
        virtual void add(const vector<Dataset*> &datasets);

    protected:
        Aggregation();
};

#endif // AGGREGATION_H_
