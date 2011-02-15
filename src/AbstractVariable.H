#ifndef ABSTRACTVARIABLE_H_
#define ABSTRACTVARIABLE_H_

#include <stdint.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "Attribute.H"
#include "DataType.H"
#include "Print.H"
#include "Values.H"
#include "Variable.H"

using std::istringstream;
using std::ostream;
using std::string;
using std::vector;

class Array;
class Dataset;
class Mask;


/**
 * Abstract base class for Variable.
 *
 * Variable operations which can be implemented using Variable's own methods
 * are done here.
 */
class AbstractVariable : public Variable
{
    public:
        virtual ~AbstractVariable();

        virtual string get_name() const = 0;

        virtual vector<Dimension*> get_dims() const = 0;
        virtual bool has_record() const;
        virtual int64_t get_nrec() const;
        virtual int64_t get_ndim() const;
        virtual vector<int64_t> get_shape() const;

        virtual vector<Attribute*> get_atts() const = 0;
        virtual int64_t num_atts() const;
        virtual string get_standard_name() const;
        virtual string get_long_name() const;
        virtual bool has_validator(int64_t index=-1) const;
        virtual Validator* get_validator(int64_t index=-1) const;
        virtual Attribute* get_att(const string &name,
                                   bool ignore_case=false) const;
        virtual Attribute* get_att(const vector<string> &names,
                                   bool ignore_case=false) const;

        virtual Dataset* get_dataset() const = 0;
        virtual vector<Mask*> get_masks() const;

        virtual DataType get_type() const = 0;

        virtual Array* read(Array *dst=NULL) const = 0;
        virtual Array* read(int64_t record, Array *dst=NULL) const = 0;

        virtual Array* iread(Array *dst=NULL) = 0;
        virtual Array* iread(int64_t record, Array *dst=NULL) = 0;

        virtual ostream& print(ostream &os) const;

        virtual int64_t get_bytes() const;

        /**
         * If there is a Mask over the record Dimension, translate the given
         * record from the index space of the mask into the index space of the
         * whole data.
         *
         * It is safe to call even if no masks are set in which case the
         * record is returned unchanged.
         *
         * @param[in] record the record to translate
         * @return the translated record
         */
        virtual int64_t translate_record(int64_t record) const;

        /**
         * Enables or disables record translation via translate_record().
         */
        virtual void set_translate_record(bool value);

    protected:
        AbstractVariable();

        /**
         * Return true if this Variable has any active Masks.
         *
         * @return true if this Variable requires subsetting
         */
        virtual bool needs_subset() const;

        /**
         * Return true if this Variable has any active, non-record Masks.
         *
         * @return true if this Variable requires subsetting
         */
        virtual bool needs_subset_record() const;

        /**
         * If this is a topology Variable and its connected Dimension has been
         * subset, we must renumber this Variable's values.
         *
         * @return true if this Variable should be renumbered
         */
        virtual bool needs_renumber() const;

        /**
         * If this is a topology Variable and its connected Dimension has been
         * subset, go ahead and renumber the given array assuming it was
         * something that was read earlier.
         *
         * @param[in] array the Array to renumber
         */
        virtual void renumber(Array *array) const;

        template <class T>
        static bool get_fill_value(const Attribute *att, T &value);

        Array* read_alloc() const;
        Array* read_alloc(int64_t record) const;

        Array* iread_alloc();
        Array* iread_alloc(int64_t record);

        bool enable_record_translation;
};


template <class T>
bool AbstractVariable::get_fill_value(const Attribute *att, T &value)
{
    if (NULL == att) {
        return false;
    } else {
        DataType type_att = att->get_type();
        if (type_att == DataType::CHAR) {
            istringstream sin(att->get_string());
            sin >> value;
            if (sin.fail()) {
                pagoda::print_zero("WARNING: could not convert _FillValue/missing_value from char");
                return false;
            }
        } else {
            if (att->get_count() != 1) {
                pagoda::print_zero("WARNING: _FillValue/missing_value incorrect count");
                return false;
            }
            att->get_values()->as(size_t(0), value);
        }
        return true;
    }
}

#endif // ABSTRACTVARIABLE_H_
