#ifndef DATASET_H_
#define DATASET_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "Attribute.H"
#include "Dimension.H"
#include "Variable.H"

class Dataset
{
    public:
        Dataset();
        Dataset(const Dataset &copy);
        virtual ~Dataset();

        void add_file(string file);

        friend ostream& operator << (ostream &os, const Dataset &other);

    protected:
        bool has_dimension(Dimension dimension) const;
        bool has_variable(Variable variable) const;
        bool has_attribute(Attribute attribute) const;

    private:
        string name;
        vector<Attribute> attributes;
        vector<Dimension> dimensions;
        vector<Variable> variables;
};

#endif // DATASET_H_
