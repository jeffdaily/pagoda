#ifndef AGGREGATIONUNION_H_
#define AGGREGATIONUNION_H_

#include <string>
#include <vector>

#include "Aggregation.H"

using std::string;
using std::vector;

class Attribute;
class Dimension;
class MaskMap;
class Variable;


/**
 * An aggregated Dataset which combines unique Dimensions and Variables by
 * name (first found, first aggregated).
 */
class AggregationUnion : public Aggregation
{
    public:
        AggregationUnion();
        virtual ~AggregationUnion();

        virtual vector<Attribute*> get_atts() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars() const;

        virtual void add(Dataset *dataset);

        virtual void set_masks(MaskMap *masks);
        virtual MaskMap* get_masks() const;

        virtual ostream& print(ostream &os) const;

    private:
        vector<Dataset*> datasets;
        vector<Attribute*> atts;
        vector<Dimension*> dims;
        vector<Variable*> vars;
        MaskMap *masks;
};

#endif // AGGREGATIONUNION_H_
