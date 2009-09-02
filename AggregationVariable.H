#ifndef AGGREGATIONVARIABLE_H_
#define AGGREGATIONVARIABLE_H_

#include <iostream>
#include <string>
#include <vector>

#include "AbstractVariable.H"
#include "DataType.H"

using std::ostream;
using std::string;
using std::vector;

class Attribute;


class AggregationVariable : public AbstractVariable
{
    public:
        AggregationVariable(Dimension *dim, Variable *var);
        virtual ~AggregationVariable();

        void add(Variable *var);

        virtual string get_name() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Attribute*> get_atts() const;

        virtual DataType get_type() const;

        virtual int get_handle();
        virtual void release_handle();
        virtual void read();
        virtual void set_record_index(size_t index);

        virtual ostream& print(ostream &os) const;

    protected:
        size_t index_var;
        int64_t index_within_var;
        Dimension *dim;         /* an AggregationDimension */
        vector<Variable*> vars; /* Variables we are concatenating */
};

#endif // AGGREGATIONVARIABLE_H_
