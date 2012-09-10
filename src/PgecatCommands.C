#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "GenericCommands.H"
#include "PgecatCommands.H"


PgecatCommands::PgecatCommands()
    :   GenericCommands()
    ,   new_unlimited_name("")
{
    init();
}


PgecatCommands::PgecatCommands(int argc, char **argv)
    :   GenericCommands()
    ,   new_unlimited_name("")
{
    init();
    parse(argc, argv);
}


PgecatCommands::~PgecatCommands()
{
}


void PgecatCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc, argv);

    if (parser.count(CommandLineOption::UNLIMITED_DIMENSION_NAME)) {
        new_unlimited_name = 
            parser.get_argument(CommandLineOption::UNLIMITED_DIMENSION_NAME);
    }
}


void PgecatCommands::init()
{
    parser.push_back(CommandLineOption::UNLIMITED_DIMENSION_NAME);
}
