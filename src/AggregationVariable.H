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
class AggregationDimension;


/**
 * An aggregated Variable whose Dimension has been joined.
 *
 * This assumes the Variable has an unlimited Dimension.
 */
class AggregationVariable : public AbstractVariable
{
    public:
        AggregationVariable(AggregationDimension *agg_dim, Variable *var);
        virtual ~AggregationVariable();

        void add(Variable *var);

        virtual string get_name() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Attribute*> get_atts() const;

        virtual DataType get_type() const;

        virtual Array* read(Array *dst);
        virtual Array* read(int64_t record, Array *dst);

        virtual Array* iread(Array *dst);
        virtual Array* iread(int64_t record, Array *dst);

        virtual ostream& print(ostream &os) const;

    protected:
        AggregationDimension *agg_dim;
        vector<Variable*> vars; /**< Variables we are concatenating */
        vector<Array*> arrays_to_copy; /**< when non-blocking completes */
        Array *array_to_fill; /**< when non-blocking completes */
};

#endif // AGGREGATIONVARIABLE_H_
