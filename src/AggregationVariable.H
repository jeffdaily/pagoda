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

        virtual bool has_fill_value(int64_t record) const;
        virtual double get_fill_value(int64_t record) const;

        virtual Dataset* get_dataset() const;

        virtual DataType get_type() const;

        virtual Array* read(Array *dst=NULL) const;
        virtual Array* read(int64_t record, Array *dst=NULL) const;

        virtual Array* iread(Array *dst=NULL);
        virtual Array* iread(int64_t record, Array *dst=NULL);

        virtual ostream& print(ostream &os) const;

    protected:
        const Aggregation *agg;
        AggregationDimension *agg_dim;
        vector<Variable*> vars; /**< Variables we are concatenating */
        vector<Array*> arrays_to_copy; /**< when non-blocking completes */
        Array *array_to_fill; /**< when non-blocking completes */

    private:
        /**
         * Reads one timestep at a time and copies to dst Array.
         */
        Array* read_per_record(Array *dst) const;

        /**
         * Reads one aggregated Variable at a time and copies to dst Array.
         */
        Array* read_per_variable(Array *dst) const;
};

#endif // AGGREGATIONVARIABLE_H_
