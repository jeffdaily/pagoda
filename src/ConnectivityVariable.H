#ifndef CONNECTIVITYVARIABLE_H_
#define CONNECTIVITYVARIABLE_H_

#include "VariableDecorator.H"

class Dimension;
class Variable;


class ConnectivityVariable : public VariableDecorator
{
    public:
        ConnectivityVariable(Variable *var, Dimension *to);
        virtual ~ConnectivityVariable();

        Dimension* get_from_dim() const;
        Dimension* get_to_dim() const;

        //virtual void reindex();

        virtual ostream& print(ostream &os) const;

    protected:
        Dimension *to;
};

#endif // CONNECTIVITYVARIABLE_H_
