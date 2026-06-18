#ifndef DREDIS_DISPATCHER_H
#define DREDIS_DISPATCHER_H

#include <string>
#include <vector>

#include "network.h"
#include "parser.h"

class Dispatcher {
public:
    void dispatch(Client& client, COMMAND& cmd);
};

extern Dispatcher dispatcher;

#endif
