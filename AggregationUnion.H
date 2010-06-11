#ifndef AGGREGATIONUNION_H_
#define AGGREGATIONUNION_H_

#include <string>
#include <vector>

#include "Aggregation.H"

using std::string;
using std::vector;

class Attribute;
class Dimension;
class Variable;


class AggregationUnion : public Aggregation
{
    public:
        AggregationUnion();
        virtual ~AggregationUnion();

        virtual vector<Attribute*> get_atts();
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars();

        virtual void add(Dataset *dataset);

        virtual ostream& print(ostream &os) const;

    protected:
        virtual void decorate_set(const vector<Variable*> &vars);

    private:
        vector<Dataset*> datasets;
        vector<Attribute*> atts;
        vector<Dimension*> dims;
        vector<Variable*> vars;
};

#endif // AGGREGATIONUNION_H_
