#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dimension.H"
#include "Variable.H"


Variable::Variable()
{
}


Variable::~Variable()
{
}


ostream& operator << (ostream &os, const Variable *other)
{
    return other->print(os);
}


/**
 * Variables are equal if their names and dims match.
 */
bool Variable::equal(const Variable *left, const Variable *right)
{
    if (left->get_name() == right->get_name()) {
        vector<Dimension*> left_dims = left->get_dims();
        vector<Dimension*> right_dims = right->get_dims();
        if (left_dims.size() == right_dims.size()) {
            for (size_t i=0; i<left_dims.size(); ++i) {
                if (!Dimension::equal(left_dims[i],right_dims[i])) {
                    return false;
                }
            }
            return true;
        }
    }
    return false;
}


/**
 * Compares two sets of variables.
 *
 * Order does not matter.
 */
bool
Variable::equal(const vector<Variable*> &left, const vector<Variable*> &right)
{
    // check vars
    if (left.size() != right.size()) {
        return false;
    }
    for (size_t i=0; i<left.size(); ++i) {
        size_t j;
        Variable *left_var = left[i];
        for (j=0; j<right.size(); j++) {
            if (Variable::equal(left_var, right[j])) {
                break;
            }
        }
        if (j >= right.size()) {
            return false;
        }
    }

    return true;
}


/**
 * Finds the Variable in the set matching the given Variable.
 */
Variable*
Variable::find(const vector<Variable*> &vars, const Variable *var)
{
    for (size_t i=0; i<vars.size(); ++i) {
        if (Variable::equal(vars[i],var)) {
            return vars[i];
        }
    }

    return NULL;
}


/**
 * Splits a vector of Variable instances into record and nonrecord vectors.
 */
void Variable::split(const vector<Variable*> &vars,
                     vector<Variable*> &record_vars,
                     vector<Variable*> &nonrecord_vars)
{
    record_vars.clear();
    nonrecord_vars.clear();
    for (vector<Variable*>::const_iterator it=vars.begin(), end=vars.end();
            it!=end; ++it) {
        Variable *var = *it;
        if (var->has_record()) {
            record_vars.push_back(var);
        } else {
            nonrecord_vars.push_back(var);
        }
    }
}
