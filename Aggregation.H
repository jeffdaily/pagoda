#ifndef AGGREGATION_H_
#define AGGREGATION_H_

#include <iostream>
    using std::ostream;
#include <vector>
    using std::vector;

#include "Dataset.H"

class Attribute;
class Dimension;
class Variable;


class Aggregation : public Dataset
{
    public:
        Aggregation();
        virtual ~Aggregation();

        virtual void add(Dataset *dataset) = 0;
        void add(const vector<Dataset*> &datasets);
};

#endif // AGGREGATION_H_
