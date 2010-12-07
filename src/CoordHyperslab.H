#ifndef COORDHYPERSLAB_H_
#define COORDHYPERSLAB_H_

#include <iostream>
#include <string>

using std::ostream;;
using std::string;

/**
 * Represents an index hyperslab for subsetting.
 */
class CoordHyperslab
{
    public:
        CoordHyperslab(const string &name, double min, double max);
        CoordHyperslab(string arg);
        CoordHyperslab(const CoordHyperslab &that);
        ~CoordHyperslab();

        string get_name() const;
        double get_min() const;
        double get_max() const;
        bool has_min() const;
        bool has_max() const;

        bool operator== (const CoordHyperslab &that) const;
        CoordHyperslab& operator= (const CoordHyperslab &that);
        friend ostream& operator<< (ostream &os,
                                    const CoordHyperslab &hyperslab);

    protected:
        string name;
        double min;
        double max;
        bool min_is_set;
        bool max_is_set;
};

#endif /* COORDHYPERSLAB_H_ */
