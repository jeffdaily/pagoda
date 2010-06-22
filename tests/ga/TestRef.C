#include <iostream>
#include <map>

using namespace std;

class Bar
{
    public:
        Bar() {
            cout << "Bar ctor default" << endl;
        }

        Bar(const Bar &that) {
            cout << "Bar ctor copy" << endl;
        }

        ~Bar() {
            cout << "Bar dtor" << endl;
        }

        Bar& operator=(const Bar &that) {
            if (this == &that) {
                cout << "Bar operator= SELF-ASSIGNMENT" << endl;
            } else {
                cout << "Bar operator=" << endl;
            }
        }
};


class Foo
{
    public:
        Foo() {
            cout << "Foo ctor default" << endl;
        }

        Foo(const Foo &that) {
            cout << "Foo ctor copy" << endl;
        }

        ~Foo() {
            cout << "Foo dtor" << endl;
        }

        Foo& operator=(const Foo &that) {
            if (this == &that) {
                cout << "Foo operator= SELF-ASSIGNMENT" << endl;
            } else {
                cout << "Foo operator=" << endl;
            }
        }

        Bar& operator[](const string &value) {
            return bars[value];
        }

    private:
        map<string,Bar> bars;
        
};


int main(int argc, char **argv)
{
    Foo foo1;
    Foo foo2 = foo1;
    Foo foo3(foo1);
    Foo foo4; foo4 = foo1; foo4 = foo4;

    Bar bar1;
    Bar bar2;

    cout << "bar1 = foo1[\"bar1\"];" << endl;
    bar1 = foo1["bar1"];
    cout << "bar1 = foo1[\"bar1\"];" << endl;
    bar1 = foo1["bar1"];
    cout << "bar_ref = foo1[\"bar1\"];" << endl;
    Bar &bar_ref = foo1["bar1"];
    cout << "bar2 = foo1[\"bar2\"];" << endl;
    bar2 = foo1["bar2"];

    return 0;
}
