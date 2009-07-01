#ifndef DATASET_H_
#define DATASET_H_

#include <iostream>
    using std::ostream;
#include <vector>
    using std::vector;

class Attribute;
class Dimension;
class Variable;

class Dataset
{
    public:
        Dataset();
        virtual ~Dataset();

        virtual vector<Attribute*> get_atts() = 0;
        virtual vector<Dimension*> get_dims() = 0;
        virtual vector<Variable*> get_vars() = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Dataset *dataset);
};

#endif // DATASET_H_
