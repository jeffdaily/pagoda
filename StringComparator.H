#ifndef STRINGCOMPARATOR_H_
#define STRINGCOMPARATOR_H_

#include <string>
    using std::string;


class StringComparator
{
    public:
        StringComparator();
        StringComparator(const string &value, bool ignore_case=true,
                bool within=false);
        StringComparator(const StringComparator &copy);
        StringComparator& operator = (const StringComparator &copy);
        virtual ~StringComparator();

        bool operator () (const string &that) const;

        void set_value(const string &value);
        void set_ignore_case(bool ignore_case=true);
        void set_within(bool within=true);

        string get_value() const;
        bool get_ignore_case() const;
        bool get_within() const;

    protected:
        string value;
        bool ignore_case;
        bool within;
};

#endif // STRINGCOMPARATOR_H_
