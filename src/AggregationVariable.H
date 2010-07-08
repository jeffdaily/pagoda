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

class Aggregation;
class Array;
class Attribute;
class AggregationDimension;
class Dataset;


/**
 * An aggregated Variable whose Dimension has been joined.
 *
 * This assumes the Variable has an unlimited Dimension.
 */
class AggregationVariable : public AbstractVariable
{
    public:
        AggregationVariable(Aggregation *agg,
                AggregationDimension *agg_dim, Variable *var);
        virtual ~AggregationVariable();

        void add(Variable *var);

        virtual string get_name() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Attribute*> get_atts() const;

        virtual Dataset* get_dataset() const;

        virtual DataType get_type() const;

        virtual Array* read(Array *dst) const;
        virtual Array* read(int64_t record, Array *dst) const;

        virtual Array* iread(Array *dst);
        virtual Array* iread(int64_t record, Array *dst);

        virtual ostream& print(ostream &os) const;

    protected:
        const Aggregation *agg;
        AggregationDimension *agg_dim;
        vector<Variable*> vars; /**< Variables we are concatenating */
        vector<Array*> arrays_to_copy; /**< when non-blocking completes */
        Array *array_to_fill; /**< when non-blocking completes */
};

#endif // AGGREGATIONVARIABLE_H_
