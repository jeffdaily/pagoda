#ifndef VARIABLE_H_
#define VARIABLE_H_

#include <iostream>
#include <string>
#include <vector>

using std::ostream;
using std::string;
using std::vector;

#include "DataType.H"

class Array;
class Attribute;
class Dataset;
class Dimension;
class Mask;


/**
 * A Variable has a name, a type, a shape, and data.
 *
 * A Variable can be thought of as a named Array.  Variables are the read-only
 * interface to a Dataset's data.
 */
class Variable
{
    public:
        virtual ~Variable();

        virtual string get_name() const = 0;

        virtual vector<Dimension*> get_dims() const = 0;
        virtual bool has_record() const = 0;
        virtual int64_t get_nrec() const = 0;
        virtual int64_t get_ndim() const = 0;
        virtual vector<int64_t> get_shape() const = 0;

        virtual vector<Attribute*> get_atts() const = 0;
        virtual int64_t num_atts() const = 0;
        virtual string get_standard_name() const = 0;
        virtual string get_long_name() const = 0;

        /**
         * Returns true if this Variable has missing data.
         *
         * Sometimes the fill/missing value may change from file to file if
         * this Variable represents an aggregation (or perhaps in the future
         * the fill/missing value may be allowed to change from record to
         * record within the same file.)  The index parameter allows for the
         * correct fill/missing value to be returned.
         *
         * @param[in] index of the record
         * @return true if this Variable has missing data.
         */
        virtual bool has_fill_value(int64_t index=-1) const = 0;

        /**
         * Returns the fill/missing value.
         *
         * It is an error to call this function without first checking
         * has_fill_value() == true.
         *
         * Sometimes the fill/missing value may change from file to file if
         * this Variable represents an aggregation (or perhaps in the future
         * the fill/missing value may be allowed to change from record to
         * record within the same file.)  The index parameter allows for the
         * correct fill/missing value to be returned.
         *
         * @param[in] index of the record
         * @return the fill/missing value
         */
        virtual double get_fill_value(int64_t index=-1) const = 0;

        virtual Attribute* get_att(const string &name,
                                   bool ignore_case=false) const = 0;
        virtual Attribute* get_att(const vector<string> &names,
                                   bool ignore_case=false) const = 0;

        virtual Dataset* get_dataset() const = 0;
        virtual vector<Mask*> get_masks() const = 0;

        virtual DataType get_type() const = 0;

        virtual Array* read(Array *dst=NULL) const = 0;
        virtual Array* read(int64_t record, Array *dst=NULL) const = 0;

        virtual Array* iread(Array *dst=NULL) = 0;
        virtual Array* iread(int64_t record, Array *dst=NULL) = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Variable *other);

        static bool equal(const Variable *left, const Variable *right);
        static bool equal(const vector<Variable*> &left,
                          const vector<Variable*> &right);
        static Variable* find(const vector<Variable*> &vars,
                              const Variable *var);

        virtual int64_t translate_record(int64_t record) const = 0;
        virtual void set_translate_record(bool value) = 0;

        virtual int64_t get_bytes() const = 0;

    protected:
        Variable();
};

#endif // VARIABLE_H_
