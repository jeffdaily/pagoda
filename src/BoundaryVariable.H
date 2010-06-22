#ifndef BOUNDARYVARIABLE_H_
#define BOUNDARYVARIABLE_H_

#include "VariableDecorator.H"

class Variable;


class BoundaryVariable : public VariableDecorator
{
    public:
        BoundaryVariable(Variable *bound, Variable *bounded_var);
        virtual ~BoundaryVariable();

        Variable* get_bounded_var() const;

        virtual ostream& print(ostream &os) const;

    protected:
        Variable *bounded_var;
};

#endif // BOUNDARYVARIABLE_H_
