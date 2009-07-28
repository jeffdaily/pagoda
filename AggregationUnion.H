#ifndef AGGREGATIONUNION_H_
#define AGGREGATIONUNION_H_

#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "Aggregation.H"

class Attribute;
class Dimension;
class Variable;


class AggregationUnion : public Aggregation
{
    public:
        AggregationUnion();
        virtual ~AggregationUnion();

        virtual vector<Attribute*> get_atts();
        virtual vector<Dimension*> get_dims();
        virtual vector<Variable*> get_vars();

        virtual void add(Dataset *dataset);
        virtual void add(const vector<Dataset*> &datasets);

        virtual ostream& print(ostream &os) const;

    protected:
        virtual void decorate(const vector<Variable*> &vars);

    private:
        vector<Dataset*> datasets;
        vector<Attribute*> atts;
        vector<Dimension*> dims;
        vector<Variable*> vars;
};

#endif // AGGREGATIONUNION_H_
