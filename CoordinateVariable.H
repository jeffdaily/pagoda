#ifndef COORDINATEVARIABLE_H_
#define COORDINATEVARIABLE_H_

#include "VariableDecorator.H"

class Variable;


class CoordinateVariable : public VariableDecorator
{
    public:
        CoordinateVariable(Variable *var);
        virtual ~CoordinateVariable();

        virtual ostream& print(ostream &os) const;
};

#endif // COORDINATEVARIABLE_H_
